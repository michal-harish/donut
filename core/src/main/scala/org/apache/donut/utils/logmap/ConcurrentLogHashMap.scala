/**
 * Donut - Recursive Stream Processing Framework
 * Copyright (C) 2015 Michal Harish
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.apache.donut.utils.logmap

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by mharis on 01/10/15.
 *
 * This is a specialised Thread-Safe HashMap based on in-memory block-storage which may be partially compressed either
 * a per-entry basis by given minimum block size which triggers compression for each value separately (SegmentDirectMemoryLZ4)
 * or at the time of compaction when segments not accessed recently can be first compressed instead of removed.
 *
 * It has a hash table index and a list of segments. Both of these its internal structures are implemented in this
 * package and are based on direct memory buffers which allows for zero-copy access although application must take
 * some extra steps to ensure zero-copy. Direct memory buffers also allow for precise and cheap statistics about
 * it's capacity, load factor and actual compression rate.
 *
 * Each segments is a single pre-allocated block of memory which shrinks over time as the entries are popped to the
 * top segment every time they are accessed. This leads to segments slowly evaporating except for the top one.
 *
 * Every attempt to allocate a new memory segment is tracked and triggers compaction if the total memory needed
 * would exceed than number of megabytes defined in the constructor of the map.
 *
 * During compaction several things may happen. Firstly each segment is compacted without re-allocation removing
 * values that were marked for deletion by entries leaving for the top segment, decreasing actual load factor.
 * Entries that are not accessed for longer and longer remain as sediment in smaller segments so compaction will
 * remember each whose actual load factor falls below 0.5. Those will be added together creating a fossil segment.
 * The segments that are filled with data that is not being accessed for long period of time may be either first
 * compressed and wait for removal due to limit on the overall memory allocated for the program,
 * or removed directly depending on the character of the data it is being used for.
 *
 */

class ConcurrentLogHashMap(val maxSizeInMb: Long, val segmentSizeMb: Int, val compressMinBlockSize: Int) {

  //TODO split compaction into asynchronous part for internal segment compaction and synchronous for merging
  // and recycling segments
  //TODO at the moment it is fixed to ByteBuffer keys and values but it should be possible to generalise into
  // ConcurrentLogHashMap[K,V] with implicit serdes such that the zero-copy capability is preserved
  //FIXME def iterator[X] returns unsafe iterator as it unlocks the indexReader right after instantiation so we need
  // to implement the underlying hashtable iterators with logical offset instead of hashPos and validate against the index

  val maxSizeInBytes = maxSizeInMb * 1024 * 1024

  type COORD = (Boolean, Short, Int)
  private[donut] val index = new VarHashTable(initialCapacityKb = 64, loadFactor = 0.6)
  private val indexLock = new ReentrantReadWriteLock
  private val indexReader = indexLock.readLock
  private val indexWriter = indexLock.writeLock

  private[logmap] val catalog = new ConcurrentSegmentCatalog(segmentSizeMb, compressMinBlockSize, (onAllocateSegment: Int) => {
    if (currentSizeInBytes + onAllocateSegment > maxSizeInBytes) {
      compact(makeAvailableNumBytes = onAllocateSegment)
    }
  })

  def numSegments = catalog.iterator.size

  def size: Int = catalog.iterator.map(_.size).sum

  def compressRatio: Double = catalog.iterator.map(_.compressRatio).toSeq match {
    case r => if (r.size == 0) 0 else (r.sum / r.size)
  }

  def currentSizeInBytes: Long = catalog.iterator.map(_.capacityInBytes.toLong).sum + index.sizeInBytes

  def load: Double = catalog.iterator.map(_.sizeInBytes.toLong).sum.toDouble / currentSizeInBytes

  def compact: Unit = compact(0)

  def contains(key: ByteBuffer): Boolean = {
    indexReader.lock
    try {
      index.contains(key)
    } finally {
      indexReader.unlock
    }
  }

  final def iterator: Iterator[(ByteBuffer, ByteBuffer)] = iterator(b => b)

  def iterator[X](mapper: (ByteBuffer) => X): Iterator[(ByteBuffer, X)] = {
    indexReader.lock
    try {
      val it = index.iterator
      new Iterator[(ByteBuffer, X)] {
        override def hasNext: Boolean = it.hasNext

        override def next(): (ByteBuffer, X) = {
          val (key: ByteBuffer, p: COORD) = it.next
          (key, catalog.getBlock(p, mapper))
        }
      }
    } finally {
      indexReader.unlock
    }
  }

  def put(key: ByteBuffer, value: ByteBuffer) = {
    val newIndexValue = catalog.append(key, value)
    indexWriter.lock
    try {
      val previousIndexValue = index.get(key)
      index.put(key, newIndexValue)
      if (previousIndexValue != null) {
        catalog.markForDeletion(previousIndexValue)
      }
    } finally {
      indexWriter.unlock
    }
  }

  final def get(key: ByteBuffer): ByteBuffer = get(key, (b: ByteBuffer) => b)
  
  def get[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    def inTransit(i: COORD) = i._1

    indexReader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case i: COORD => {
          if (catalog.isInCurrentSegment(i) || inTransit(i)) {
            return catalog.getBlock(i, mapper)
          } else {
            var oldIndexValue: COORD = null
            var newIndexValue: COORD = null
            indexReader.unlock
            indexWriter.lock
            try {
              oldIndexValue = index.get(key)
              if (inTransit(oldIndexValue)) {
                return catalog.getBlock(oldIndexValue, mapper)
              }
              index.flag(key, true)
              newIndexValue = catalog.alloc(catalog.sizeOf(oldIndexValue))
            } catch {
              case e: Throwable => {
                if (oldIndexValue != null) index.flag(key, false)
                if (newIndexValue != null) catalog.dealloc(newIndexValue)
                throw e
              }
            } finally {
              indexReader.lock
              indexWriter.unlock
            }

            catalog.move(oldIndexValue, newIndexValue)

            indexReader.unlock
            indexWriter.lock
            try {
              index.put(key, newIndexValue)
            } finally {
              indexReader.lock
              indexWriter.unlock
            }
            catalog.getBlock(newIndexValue, mapper)
          }
        }
      }
    } finally {
      indexReader.unlock
    }
  }

  def compact(makeAvailableNumBytes: Int): Unit = {
    catalog.compact
    if (currentSizeInBytes + makeAvailableNumBytes > maxSizeInBytes) {
      indexWriter.lock
      try {
        var bytesToRemove = currentSizeInBytes + makeAvailableNumBytes - maxSizeInBytes
        var nSegmentsToRemove = 0
        catalog.iterator.zipWithIndex.foreach { case (segment, s) => {
          if (bytesToRemove > 0) {
            bytesToRemove -= segment.capacityInBytes
            nSegmentsToRemove += 1
          }
        }
        }
        if (nSegmentsToRemove > 0) {
          index.update((v: COORD) => {
            v._2 match {
              case s if (s < nSegmentsToRemove) => null
              case segment => (v._1, (segment - nSegmentsToRemove).toShort, v._3)
            }
          })
          catalog.dropOldestSegments(nSegmentsToRemove)
        }
      } finally {
        indexWriter.unlock
      }
    }
  }


}




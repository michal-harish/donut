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
 * This is a specialised Thread-Safe HashMap based on in-memory block storage which may be partially compressed
 * dif a given minimum block size and load fraction conditions are met. The compression must be triggered as part
 * of compaction. If the values that have been compressed are accessed again, the blocks containing them are
 * uncompressed into the latest segment where they travel normally in the history of log until they meet compression
 * criteria again.
 *
 * It has a hash table index and a list of segments. Both of these internal structures are implemented in this
 * package and are based on direct memory buffers which allows for zero-copy access although application must take
 * some extra steps to ensure zero-copy. Direct memory buffers also allow for precise and cheap statistics about
 * it's capacity, load factor and actual compression rate.
 *
 * Each segments is a single pre-allocated block of memory which shrinks over time as the entries are popped to the
 * top segment every time they are accessed. This leads to segments slowly evaporating except for the top one.
 *
 * Every attempt to allocate a new memory segment is tracked and triggers compaction if the total memory needed
 * would exceed the number of megabytes defined in the constructor of the map.
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

class ConcurrentLogHashMap(
                            val maxSizeInMb: Long,
                            val segmentSizeMb: Int,
                            val compressMinBlockSize: Int,
                            val indexLoadFactor: Double = 0.7) {

  //TODO at the moment it is fixed to ByteBuffer keys and values but it should be possible to generalise into
  // ConcurrentLogHashMap[K,V] with implicit serdes such that the zero-copy capability is preserved

  //TODO def iterator[X] returns unsafe iterator as it unlocks the indexReader right after instantiation so we need
  // to implement the underlying hashtable iterators with logical offset instead of hashPos and validate against the index

  //TODO when compacting, if there are 2 or more segments with joint load <= 1.0 merge into one of them and recycle the others

  //TODO custom class of ByteBuffer for lz4 buffers could remember which block is it pointing to but if we'll implement
  //always decompressing the entire block into the current segmet that doesn't need to happen

  // TODO At the moment, if a block is being moved (by get-touch) from a compressed group it will also remain in the
  // compressed group - what should really happen is that since we're uncompressing the block it would make sense
  // to first re-store all blocks it contains within the current segment not just the one being requested.

  //TODO generalise hash table into  VarHashTable[K] and use K.hashCode so that we can do correction for 0 and
  // Int.MinValue hashCodes transparently

  val maxSizeInBytes = maxSizeInMb * 1024 * 1024

  type COORD = (Boolean, Short, Int)
  private[donut] val index = new VarHashTable(initialCapacityKb = 64, indexLoadFactor)
  private val indexLock = new ReentrantReadWriteLock
  private val indexReader = indexLock.readLock
  private val indexWriter = indexLock.writeLock

  private[logmap] val catalog = new ConcurrentSegmentCatalog(segmentSizeMb, compressMinBlockSize, (onAllocateSegment: Int) => {
    if (totalSizeInBytes + onAllocateSegment > maxSizeInBytes) {
      compact
      val requireBytes = totalSizeInBytes + onAllocateSegment - maxSizeInBytes
      //      println(s"Recycling: current total size ${totalSizeInBytes / 1024 / 1024} Mb " +
      //        s"plus required ${onAllocateSegment / 1024 / 1024} Mb " +
      //        s"is > $maxSizeInMb Mb total maximum allowed")
      //      println(s"Recycling: requesting ${requireBytes / 1024/ 1024}Mb")
      recycleNumBytes(requireBytes)
      if (totalSizeInBytes + onAllocateSegment > maxSizeInBytes) {
        throw new OutOfMemoryError(s"Could not ensure ${onAllocateSegment} will be available. " +
          s"Current map size ${totalSizeInBytes / 1024 / 1024} Mb (of that index ${indexSizeInBytes / 2014 / 2014} Mb")
      }
    }
  })

  def numSegments = catalog.iterator.size

  def size: Int = catalog.iterator.map(_._2.size).sum

  def compact: Unit = catalog.compact

  def applyCompression(fraction: Double): Unit = catalog.compress(maxSizeInMb, fraction)

  def compressRatio: Double = catalog.iterator.map(_._2.compressRatio).toSeq match {
    case r => if (r.size == 0) 0 else (r.sum / r.size)
  }

  def logSizeInBytes: Long = catalog.iterator.map(_._2.totalSizeInBytes.toLong).sum

  def indexSizeInBytes: Long = index.sizeInBytes

  def totalSizeInBytes: Long = catalog.totalCapacityInBytes + indexSizeInBytes

  def load: Double = catalog.iterator.map(_._2.usedBytes.toLong).sum.toDouble / catalog.totalCapacityInBytes

  def contains(key: ByteBuffer): Boolean = {
    indexReader.lock
    try {
      index.contains(key)
    } finally {
      indexReader.unlock
    }
  }

  def seek[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    indexReader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case i: COORD => catalog.getBlock(i, mapper)
      }
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

  def recycleNumBytes(numBytesToRecyecle: Long): Unit = {
    var bytesToRemove = numBytesToRecyecle
    indexWriter.lock
    try {
      var segmentsRemoved = new scala.collection.mutable.HashSet[Short]()
      catalog.iterator.foreach { case (s, segment) => {
        if (bytesToRemove > 0) {
          bytesToRemove -= segment.totalSizeInBytes
          segmentsRemoved += s
          catalog.recycleSegment(s)
        }
      }
      }
      if (segmentsRemoved.size > 0) {
        index.update((pointer: COORD) => {
          pointer._2 match {
            case removedSegment if (segmentsRemoved.contains(removedSegment)) => null
            case retainedSegment => pointer
          }
        })
      }
    } finally {
      indexWriter.unlock
    }

  }

  def printStats = {
    println(s"LOGHASHMAP STATS: num.entires = ${size}  total.mb = ${totalSizeInBytes / 1024 / 1024} Mb  compression = ${compressRatio} (of that index: ${indexSizeInBytes / 1024 / 1024 } Mb with load ${index.load * 100.0} %})")
    catalog.printStats
  }

}




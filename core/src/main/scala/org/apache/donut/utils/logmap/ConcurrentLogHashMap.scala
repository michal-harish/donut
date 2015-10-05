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
 * on a per-entry basis or per-segment basis. Its most important feature is that each time an entry is accessed
 * it is propagated to the latest segments eventually leaving some segments empty which are then recycled without
 * any memory re-allocation or, depending on configuration, the older segments are compressed as they `age`.
 *
 * Both it's hash table and value collections are implemented in this package and are based on direct memory buffers
 * which allows for zero-copy accesss although application must take some extra steps to ensure zero-copy. Direct
 * memory buffers also allow for precise control and statistics about it's capacity and load factor.
 *
 *
 * TODO: At the moment it is fixed to ByteBuffer keys and values but it should be possible to
 * TODO  generalise into ConcurrentLogHashMap[K,V] with implicit serdes such that the zero-copy capability is preserved
 */

class ConcurrentLogHashMap(val maxSizeInMb: Long, val segmentSizeMb: Int) {

  //TODO run compact(0) in the background

  val maxSizeInBytes = maxSizeInMb * 1024 * 1024

  type COORD = (Boolean, Short, Int)
  private[donut] val index = new VarHashTable(initialCapacityKb = 64, loadFactor = 0.6)
  private val indexLock = new ReentrantReadWriteLock
  private val indexReader = indexLock.readLock
  private val indexWriter = indexLock.writeLock

  private[logmap] val catalog = new ConcurrentSegmentCatalog(segmentSizeMb, compressMinBlockSize = 4096, (onAllocateSegment: Int) => {
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

  final def get(key: ByteBuffer): ByteBuffer = get(key, (b: ByteBuffer) => b)

  final def iterator: Iterator[(ByteBuffer, ByteBuffer)] = iterator(b => b)

  def put(key: ByteBuffer, value: ByteBuffer) = {
    val newIndexValue = catalog.append(key, value)
    indexWriter.lock
    try {
      val previousIndexValue = index.get(key)
      index.put(key, newIndexValue)
      if (previousIndexValue != null) {
        catalog.removeBlock(previousIndexValue)
      }
    } finally {
      indexWriter.unlock
    }
  }

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

  def iterator[X](mapper: (ByteBuffer) => X): Iterator[(ByteBuffer, X)] = {
    indexReader.lock
    try {
      //FIXME this iterator is not safe as it unlocks the indexReader after instantiation so we need to implement the underlying hashtable iterators with logical offset instead of hashPos so it behaves like ConcurrentHashMap interator
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
          catalog.dropFirstSegments(nSegmentsToRemove)
        }
      } finally {
        indexWriter.unlock
      }
    }
  }


}




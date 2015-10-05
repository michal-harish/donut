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
 * TODO: At the moment it is fixed to ByteBuffer keys and values but it should be possible to
 * TODO: generalise into ConcurrentLogHashMap[K,V] with implicit serdes such that the zero-copy capability is preserved
 */

class ConcurrentLogHashMap(val segmentSizeMb: Int) {

  private[logmap] val catalog = new ConcurrentSegmentCatalog(segmentSizeMb, compressMinBlockSize = 10240)

  def numSegments = catalog.iterator.size

  def size: Int = catalog.iterator.map(_.size).sum

  def compressRatio: Double = catalog.iterator.map(_.compressRatio).toSeq match {case r => if (r.size == 0) 0 else (r.sum / r.size)}

  def capacity: Long =  catalog.iterator.map(_.capacityInBytes.toLong).sum + index.sizeInBytes

  def sizeInBytes: Long = catalog.iterator.map(_.sizeInBytes.toLong).sum + index.sizeInBytes

  def load: Double = sizeInBytes.toDouble / capacity

  //TODO run compaction in the background
  def compact = catalog.compact

  //TODO configuration for VarHashTable index so that we do not grow it pointlessly if we know how big it's going to get
  type COORD = (Boolean, Short, Int)
  private val index = new VarHashTable(initialCapacityKb = 64, loadFactor = 0.5)
  private val indexLock = new ReentrantReadWriteLock
  private val indexReader = indexLock.readLock
  private val indexWriter = indexLock.writeLock

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

  def get(key: ByteBuffer): ByteBuffer = get(key, (b: ByteBuffer) => b)

  def get[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    def inTransit(i: COORD) = i._1

    indexReader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case i: COORD => {
          if (catalog.isInLastSegment(i) || inTransit(i)) {
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

}




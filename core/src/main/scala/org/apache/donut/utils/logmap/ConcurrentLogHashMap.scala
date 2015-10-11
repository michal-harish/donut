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
import scala.collection.JavaConverters._

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

  private def newSegmentInstance = new SegmentDirectMemoryLZ4(segmentSizeMb, compressMinBlockSize)

  type COORD = (Boolean, Short, Int)

  private[logmap] val index = new VarHashTable(initialCapacityKb = 64, indexLoadFactor)
  private val indexLock = new ReentrantReadWriteLock
  private val indexReader = indexLock.readLock
  private val indexWriter = indexLock.writeLock

  private val segmentsLock = new ReentrantReadWriteLock
  @volatile private var currentSegment: Short = 0
  private val segmentIndex = new java.util.ArrayList[Short]
  private val segments = new java.util.ArrayList[Segment]() {
    add(newSegmentInstance)
    segmentIndex.add(0)
    currentSegment = 0
  }

  def numSegments = segments.size

  def size: Int = {
    segmentsLock.readLock.lock
    try {
      return segments.asScala.map(_.size).sum
    } finally {
      segmentsLock.readLock.unlock
    }
  }

  def compressRatio: Double = {
    segmentsLock.readLock.lock
    try {
      return segments.asScala.map(_.compressRatio).toSeq match {
        case r => if (r.size == 0) 0 else (r.sum / r.size)
      }
    } finally {
      segmentsLock.readLock.unlock
    }
  }

  def indexSizeInBytes: Long = index.sizeInBytes

  def logSizeInBytes: Long = {
    segmentsLock.readLock.lock
    try {
      return segmentIndex.asScala.map(s => segments.get(s).totalSizeInBytes.toLong).sum
    } finally {
      segmentsLock.readLock.unlock
    }

  }

  def totalSizeInBytes: Long = logSizeInBytes + indexSizeInBytes

  def load: Double = totalSizeInBytes.toDouble / maxSizeInBytes

  def printStats: Unit = {
    segmentsLock.readLock.lock
    try {
      println(s"LOGHASHMAP: index.size = ${index.size} seg.entires = ${size}  total.mb = ${totalSizeInBytes / 1024 / 1024} Mb  compression = ${compressRatio} (of that index: ${indexSizeInBytes / 1024 / 1024} Mb with load ${index.load * 100.0} %})")
      segmentIndex.asScala.reverse.foreach(s => segments.get(s).printStats(s))
      segments.asScala.filter(segment => !segmentIndex.asScala.exists(i => segments.get(i) == segment)).foreach(s => s.printStats(-1))
    } finally {
      segmentsLock.readLock.unlock
    }
  }

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
          (key, getBlock(p, mapper))
        }
      }
    } finally {
      indexReader.unlock
    }
  }

  def put(key: ByteBuffer, value: ByteBuffer): Unit = {

    indexReader.lock
    try {
      val existingIndexValue = index.get(key)

      if (existingIndexValue != null) {
        segmentsLock.readLock.lock
        try {
          if (existingIndexValue._2 == currentSegment) {
            segments.get(currentSegment).put(existingIndexValue._3, value)
            return
          }
        } finally {
          segmentsLock.readLock.unlock
        }
      }
    } finally {
      indexReader.unlock
    }

    indexWriter.lock
    try {
      val existingIndexValue = index.get(key)
      if (existingIndexValue != null) {
        segmentsLock.readLock.lock
        try {
          if (existingIndexValue._2 == currentSegment) {
            segments.get(currentSegment).put(existingIndexValue._3, value)
            return
          }
        } finally {
          segmentsLock.readLock.unlock
        }
      }
      val newIndexValue = allocBlock(value.remaining)
      segments.get(newIndexValue._2).put(newIndexValue._3, value)

      index.put(key, newIndexValue)
      if (existingIndexValue != null) {
        dealloc(existingIndexValue)
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
          if (i._2 == currentSegment || inTransit(i)) {
            return getBlock(i, mapper)
          } else {
            var oldIndexValue: COORD = null
            var newIndexValue: COORD = null
            indexReader.unlock
            indexWriter.lock
            try {
              oldIndexValue = index.get(key)
              if (inTransit(oldIndexValue)) {
                return getBlock(oldIndexValue, mapper)
              }
              index.flag(key, true)
              newIndexValue = allocBlock(sizeOfBlock(oldIndexValue))
            } catch {
              case e: Throwable => {
                if (oldIndexValue != null) index.flag(key, false)
                if (newIndexValue != null) dealloc(newIndexValue)
                throw e
              }
            } finally {
              indexReader.lock
              indexWriter.unlock
            }

            //copy old block to the newly allocated block while unlocked (the new one is not visible in the index yet)
            val srcSegment = segments.get(oldIndexValue._2)
            val dstSegment = segments.get(newIndexValue._2)
            srcSegment.get[Unit](oldIndexValue._3, (b) => dstSegment.setUnsafe(newIndexValue._3, b))

            indexReader.unlock
            indexWriter.lock
            try {
              index.put(key, newIndexValue)
            } finally {
              indexReader.lock
              indexWriter.unlock
            }

            //after moving the index pointer, remove the old block
            dealloc(oldIndexValue)

            getBlock(newIndexValue, mapper)
          }
        }
      }
    } finally {
      indexReader.unlock
    }
  }

  def compact: Unit = {
    segmentsLock.readLock.lock
    try {
      for (s <- 0 to (segments.size - 1)) segments.get(s).compact
    } finally {
      segmentsLock.readLock.unlock
    }
  }

  def applyCompression(fraction: Double): Unit = {
    val sizeThreshold = (maxSizeInBytes * (1.0 - fraction)).toInt

    segmentsLock.readLock.lock
    try {
      var cumulativeSize = 0
      segmentIndex.asScala.map(segments.get(_)).foreach(segment => {
        cumulativeSize += segment.usedBytes
        if (cumulativeSize > sizeThreshold) {
          segment.compress
        }
      })
    } finally {
      segmentsLock.readLock.unlock
    }
  }

  private def getBlock[X](p: COORD, mapper: ByteBuffer => X): X = {
    segments.get(p._2).get(p._3, mapper)
  }

  private def sizeOfBlock(p: COORD): Int = {
    segments.get(p._2).sizeOf(p._3)
  }


  private def dealloc(p: COORD): Unit = {
    segments.get(p._2).remove(p._3)
  }

  /**
   * allocBlock
   * @param valueSize
   * @return
   */
  private def allocBlock(valueSize: Int): COORD = {
    segmentsLock.readLock.lock
    try {
      val newBlock = segments.get(currentSegment).alloc(valueSize)
      if (newBlock >= 0) {
        return (false, currentSegment, newBlock)
      }
    } finally {
      segmentsLock.readLock.unlock
    }

    //could not allocate block in the current segment, need to allocate new segment
    segmentsLock.writeLock.lock
    try {
      //double check if we still can't crete in current segment after acquiring write lock
      val newBlock = segments.get(currentSegment).alloc(valueSize)
      if (newBlock >= 0) {
        return (false, currentSegment, newBlock)
      }

      //now we can be sure no other thread has created new segment
      currentSegment = -1

      val bytesToAllocate = segmentSizeMb * 1024 * 1024 + index.initialCapacityKb ^ 2 // TODO estimate of index
      if (totalSizeInBytes + bytesToAllocate > maxSizeInBytes) {
        recycleNumBytes(totalSizeInBytes + bytesToAllocate - maxSizeInBytes)
      }

      for (s <- 0 to segments.size - 1) {
        if (segments.get(s).size == 0) {
          currentSegment = s.toShort
          segmentIndex.remove(currentSegment.asInstanceOf[Object])
        }
      }

      if (currentSegment == -1) {
        segments.add(newSegmentInstance)
        currentSegment = (segments.size - 1).toShort
      }

      segmentIndex.add(currentSegment)
      segments.get(currentSegment).alloc(valueSize) match {
        case -1 => throw new IllegalArgumentException(s"Could not allocate block of length `${valueSize / 1024}` Kb in an empty segment of size " + segmentSizeMb + " Mb")
        case newBlock => return (false, currentSegment, newBlock)
      }
    } finally {
      segmentsLock.writeLock.unlock
    }
  }

  private def recycleNumBytes(numBytesToRecycle: Long): Unit = {
    //    println(s"Recycling request for ${numBytesToRecycle / 1024 / 1024} Mb")
    var bytesToRemove = numBytesToRecycle
    var segmentsRemoved = new scala.collection.mutable.HashSet[Short]()

    segmentIndex.asScala.map(s => (s, segments.get(s))).foreach { case (s, segment) => {
      if (bytesToRemove > 0) {
        bytesToRemove -= segment.totalSizeInBytes
        if (s != currentSegment) {
          println(s"Recycling segment ${s}")
          segmentsRemoved += s
          segmentIndex.remove(s.asInstanceOf[Object])
          segments.get(s).recycle
        }
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
    //    println(s"Size after recycling = ${totalSizeInBytes / 1024 / 1024} Mb")
    if (totalSizeInBytes + numBytesToRecycle > maxSizeInBytes) {
      printStats
      throw new OutOfMemoryError(s"LogHashMap could not ensure ${numBytesToRecycle / 1024 / 1024} Mb will be available. " +
        s"Current map size ${totalSizeInBytes / 1024 / 1024} Mb (of that index ${indexSizeInBytes / 2014 / 2014} Mb")
    }

  }

}




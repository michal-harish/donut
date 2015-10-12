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
 * if a given minimum block size and load fraction conditions are met. The compression must be triggered as part
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
 * Every attempt to allocate a new block of memory is tracked and triggers compaction under several conditions.
 * First, if the compaction factor of the current block is large enough, that is by compacting the current segment
 * we will free significant amount of memory, the current segment is compacted. If the current segment is compact
 * enough and the new block doesn't fit within it, request for new segment allocation is made which can further
 * trigger compression and segment recycling if the total memory usage of the hash map is larger than the maximum
 * capacity given in the constructor.
 *
 */

class ConcurrentLogHashMap(
                            val maxSizeInMb: Long,
                            val segmentSizeMb: Int,
                            val compressMinBlockSize: Int,
                            val indexLoadFactor: Double = 0.7) {

  //TODO design the compression scheme and trigger followed by a merge of segments with joint load factor =< 1.0
  // - At the moment, if a block is being moved (by get-touch) from a compressed group it will also remain in the
  // compressed group - what should really happen is that since we're uncompressing the block it would make sense
  // to first re-store all blocks it contains within the current segment not just the one being requested.
  // - a custom class of ByteBuffer for lz4 buffers could remember which block is it pointing to but if we'll implement
  // always decompressing the entire block into the current segmet that doesn't need to happen

  //TODO def iterator[X] returns unsafe iterator as it unlocks the indexReader right after instantiation so we need
  // to implement the underlying hashtable iterators with logical offset instead of hashPos and validate in the index

  //TODO generalise hash table into  VarHashTable[K] and use K.hashCode so that we can do correction for 0 and
  // Int.MinValue hashCodes transparently

  //TODO at the moment it is fixed to ByteBuffer keys and values but it should be possible to generalise into
  // ConcurrentLogHashMap[K,V] with implicit serdes such that the zero-copy capability is preserved

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

  def capacityInBytes: Long = {
    segmentsLock.readLock.lock
    try {
      return segments.asScala.map(segment => segment.totalSizeInBytes.toLong).sum + index.sizeInBytes
    } finally {
      segmentsLock.readLock.unlock
    }
  }

  def totalSizeInBytes: Long = {
    segmentsLock.readLock.lock
    try {
      return segmentIndex.asScala.map(s => segments.get(s).totalSizeInBytes.toLong).sum + index.sizeInBytes
    } finally {
      segmentsLock.readLock.unlock
    }
  }

  def load: Double = totalSizeInBytes.toDouble / maxSizeInBytes

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
      val newIndexValue = allocBlock(if (value == null) 0 else value.remaining)
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
      for (s <- 0 to (segments.size - 1)) segments.get(s).compact(0)
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
      val segment = segments.get(currentSegment)
      if (segment.compactFactor >= 3.0) {
        segment.compact(3.0)
      }
      val newBlock = segment.alloc(valueSize)
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

      //then see if recycling is required
      val sample = segments.get(currentSegment)
      val estimateRequiredBytes = segmentSizeMb * 1024 * 1024 + (sample.totalSizeInBytes - sample.capacity)
      if (totalSizeInBytes + estimateRequiredBytes > maxSizeInBytes) {
        recycleNumBytes(totalSizeInBytes + estimateRequiredBytes - maxSizeInBytes)
      }

      currentSegment = -1

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
        case -1 => throw new IllegalArgumentException(s"Could not allocate block of length `${valueSize / 1024}` Kb " +
          s"in an empty segment of size " + segmentSizeMb + " Mb")
        case newBlock => return (false, currentSegment, newBlock)
      }
    } finally {
      segmentsLock.writeLock.unlock
    }
  }

  private def recycleNumBytes(numBytesToRecycle: Long): Unit = {
    var bytesToRemove = numBytesToRecycle
    var segmentsRemoved = new scala.collection.mutable.HashSet[Short]()

    //drop segments that no longer fit in the max size (due to index growing)
    while (capacityInBytes > maxSizeInBytes) {
      if (segments.size <= 1) {
        throw new OutOfMemoryError(
          s"LogHashMap could not ensure ${numBytesToRecycle / 1024 / 1024} Mb will be available. " +
          s"Current map size ${totalSizeInBytes / 1024 / 1024} Mb (of that index ${index.sizeInBytes / 2014 / 2014} Mb")
      }
      val s = (segments.size - 1).toShort
      segmentIndex.remove(s.asInstanceOf[Object])
      bytesToRemove -= segments.get(s).totalSizeInBytes
      segmentsRemoved += s
      segments.remove(s)
    }

    segmentIndex.asScala.map(s => (s, segments.get(s))).foreach { case (s, segment) => {
      if (bytesToRemove > 0 && s != currentSegment) {
          bytesToRemove -= segment.totalSizeInBytes
          segmentsRemoved += s
          segmentIndex.remove(s.asInstanceOf[Object])
          segments.get(s).recycle
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
  }

  def printStats: Unit = {
    segmentsLock.readLock.lock
    try {
      println(s"LOGHASHMAP: index.size = ${index.size} seg.entires = ${size} " +
        s"total.capacity = ${capacityInBytes / 1024 / 1024} Mb " +
        s"current.memory = ${size}" +
        s"(of that index: ${index.sizeInBytes / 1024 / 1024} Mb with load factor ${index.load}})" +
        s", compression = ${compressRatio} ")
      segmentIndex.asScala.reverse.foreach(s => segments.get(s).printStats(s))
      segments.asScala.filter(segment => !segmentIndex.asScala.exists(i => segments.get(i) == segment))
        .foreach(s => s.printStats(-1))
    } finally {
      segmentsLock.readLock.unlock
    }
  }
}

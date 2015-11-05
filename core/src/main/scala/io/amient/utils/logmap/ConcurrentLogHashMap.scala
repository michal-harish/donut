/**
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

package io.amient.utils.logmap

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.slf4j.LoggerFactory

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

  //TODO collect statistics about size, capacity and load atomically rather than with iterators

  //TODO design the compression scheme and trigger followed by a merge of segments with joint load factor =< 1.0
  // OPTION A: If a block is being moved (by get-touch) from a compressed group it will also remain in the
  // compressed group - what should really happen is that since we're uncompressing the block it would make sense
  // to first re-store all blocks it contains within the current segment not just the one being requested.
  // OPTION B:  a custom class of ByteBuffer for lz4 buffers could remember which block is it pointing
  // OPTION C: compression managed by the map class, segment not aware what is storing - this way we can do some
  // sophisticated compress-and-merge compactions truly freeing segments for recycling

  //TODO generalise hash table into  VarHashTable[K] and use K.hashCode so that we can do correction for 0 and
  // Int.MinValue hashCodes transparently. Also atm the first 4 bytes of any key:ByteBuffer
  // is taken to be the hashCode of the key which is sort of built around Vid implementation.

  //TODO at the moment it is fixed to ByteBuffer keys and values but it should be possible to generalise into
  // ConcurrentLogHashMap[K,V] with implicit serdes such that the zero-copy capability is preserved

  private val log = LoggerFactory.getLogger(classOf[ConcurrentLogHashMap])

  val maxSizeInBytes = maxSizeInMb * 1024 * 1024

  private def newSegmentInstance = new SegmentDirectMemoryLZ4(segmentSizeMb, compressMinBlockSize)

  type COORD = (Boolean, Short, Int)

  final private[logmap] val index = new VarHashTable(initialCapacityKb = 64, indexLoadFactor)

  @volatile private[logmap] var currentSegment: Short = 0
  final private val segmentIndex = new java.util.ArrayList[Short]
  final private val segments = new java.util.ArrayList[Segment]() {
    add(newSegmentInstance)
    segmentIndex.add(0)
    currentSegment = 0
  }

  final private val lock = new ReentrantReadWriteLock

  final private val reader = lock.readLock

  final private val writer = lock.writeLock

  def numSegments = segments.size

  def size: Int = {
    reader.lock
    try {
      var sum = 0
      var i = segmentIndex.size
      while (i > 0) {
        i -= 1
        sum += segments.get(segmentIndex.get(i)).size
      }
      return sum
    } finally {
      reader.unlock
    }
  }

  def compressRatio: Double = {
    reader.lock
    try {
      if (segments.size == 0) {
        return 0
      } else {
        var sum = 0.0
        var i = segments.size
        while (i > 0) {
          i -= 1
          sum += segments.get(segmentIndex.get(i)).compressRatio
        }
        return sum / segments.size
      }
    } finally {
      reader.unlock
    }
  }

  def capacityInBytes: Long = {
    reader.lock
    try {
      var i = segmentIndex.size
      var sum = 0L
      while (i > 0) {
        i -= 1
        sum += segments.get(segmentIndex.get(i)).totalSizeInBytes.toLong
      }
      return sum + index.sizeInBytes
    } finally {
      reader.unlock
    }
  }

  def totalSizeInBytes: Long = {
    reader.lock
    try {
      var sum = 0L
      var i = segmentIndex.size
      while (i > 0) {
        i -= 1
        sum += segments.get(segmentIndex.get(i)).totalSizeInBytes.toLong
      }
      return sum + index.sizeInBytes
    } finally {
      reader.unlock
    }
  }

  def load: Double = totalSizeInBytes.toDouble / maxSizeInBytes

  def contains(key: ByteBuffer): Boolean = {
    reader.lock
    try {
      index.contains(key)
    } finally {
      reader.unlock
    }
  }

  /**
   * safe iterator
   * @param f function to apply to each key-value bytebuffer pair
   */
  def foreach[X](f: (ByteBuffer, ByteBuffer) => X): Unit = {
    val it = index.iterator
    while (it.hasNext) {
      reader.lock
      try {
        val (key: ByteBuffer, p: COORD) = it.next
        val value: ByteBuffer = getBlock(p, (b) => b)
        f(key, value)
      } finally {
        reader.unlock
      }
    }
  }

  /**
   * safe generic iterator which takes a mapper function to convert the key-value ByteBuffer pair
   * @param f function to apply to each key-value bytebuffer pair
   * @tparam X
   * @return
   */
  def map[X](f: (ByteBuffer, ByteBuffer) => X): Iterator[X] = {
    reader.lock
    try {
      val it = index.iterator
      new Iterator[X] {
        override def hasNext: Boolean = it.hasNext

        override def next(): X = {
          reader.lock
          try {
            val (key: ByteBuffer, p: COORD) = it.next
            val value: ByteBuffer = getBlock(p, (b) => b)
            f(key, value)
          } finally {
            reader.unlock
          }
        }
      }
    } finally {
      reader.unlock
    }
  }

  def put(key: ByteBuffer, value: ByteBuffer): Unit = {
    reader.lock
    try {
      val existingIndexValue = index.get(key)
      if (existingIndexValue != null) {
        if (existingIndexValue._2 == currentSegment) {
          segments.get(currentSegment).put(existingIndexValue._3, value)
          return
        }
      }
    } finally {
      reader.unlock
    }

    writer.lock
    try {
      val existingIndexValue = index.get(key)
      if (existingIndexValue != null) {
        if (existingIndexValue._2 == currentSegment) {
          if (segments.get(currentSegment).put(existingIndexValue._3, value) >= 0) {
            //optimized case where we either overwrite literally the same memory position
            //or second best case of compaction-less append..
            //any other case needs to allocate a new block by means of potential compaction below
            return
          }
        }
      }
      val newIndexValue = allocBlock(index.bytesToGrow(key), if (value == null) 0 else value.remaining)
      segments.get(newIndexValue._2).put(newIndexValue._3, value)
      //after the allocBlock the index may have changed by means of recycling segments so re-check existing
      val checkedExistingValue = if (existingIndexValue == null) null else index.get(key)
      index.put(key, newIndexValue)
      if (checkedExistingValue != null) {
        dealloc(checkedExistingValue)
      }
    } finally {
      writer.unlock
    }
  }


  def get[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    reader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case i: COORD => getBlock(i, mapper)
      }
    } finally {
      reader.unlock
    }
  }

  def touch[X](key: ByteBuffer, mapper: (ByteBuffer => X)): X = {
    def inTransit(i: COORD) = i._1

    reader.lock
    try {
      index.get(key) match {
        case null => null.asInstanceOf[X]
        case i: COORD => {
          if (i._2 == currentSegment || inTransit(i)) {
            return getBlock(i, mapper)
          } else {
            var oldIndexValue: COORD = null
            var newIndexValue: COORD = null
            reader.unlock
            writer.lock
            try {
              oldIndexValue = index.get(key)
              if (inTransit(oldIndexValue)) {
                return getBlock(oldIndexValue, mapper)
              }
              index.setTransitFlag(key, true)
              newIndexValue = allocBlock(0, sizeOfBlock(oldIndexValue))
            } catch {
              case e: Throwable => {
                if (oldIndexValue != null) index.setTransitFlag(key, false)
                if (newIndexValue != null) dealloc(newIndexValue)
                throw e
              }
            } finally {
              reader.lock
              writer.unlock
            }

            //copy old block to the newly allocated block while unlocked (the new one is not visible in the index yet)
            val srcSegment = segments.get(oldIndexValue._2)
            val dstSegment = segments.get(newIndexValue._2)
            srcSegment.get[Unit](oldIndexValue._3, (b) => dstSegment.setUnsafe(newIndexValue._3, b))

            reader.unlock
            writer.lock
            try {
              index.put(key, newIndexValue)
              dealloc(oldIndexValue)
            } finally {
              reader.lock
              writer.unlock
            }

            getBlock(newIndexValue, mapper)
          }
        }
      }
    } finally {
      reader.unlock
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
  private def allocBlock(indexBytesToGrow: Int, valueSize: Int): COORD = {
    if (totalSizeInBytes + indexBytesToGrow < maxSizeInBytes) {
      //try the optimal case without new segment allocation
      reader.lock
      try {
        val segment = segments.get(currentSegment)
        segment.compact(0.01)
        val newBlock = segment.alloc(valueSize)
        if (newBlock >= 0) {
          return (false, currentSegment, newBlock)
        }
      } finally {
        reader.unlock
      }
    }

    //could not allocate block in the current segment, need to allocate new segment
    writer.lock
    try {
      //double check if we still can't crete in current segment after acquiring write lock
      val newBlock = segments.get(currentSegment).alloc(valueSize)
      if (newBlock >= 0) {
        return (false, currentSegment, newBlock)
      }

      //then see if recycling is required
      val sample = segments.get(currentSegment)
      val estimateRequiredBytes = segmentSizeMb * 1024 * 1024 + indexBytesToGrow + (sample.totalSizeInBytes - sample.capacity)
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
      writer.unlock
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
      index.update((key: ByteBuffer, pointer: COORD) => {
        pointer._2 match {
          case removedSegment if (segmentsRemoved.contains(removedSegment)) => {
            try {
              onEvictEntry(key)
            } catch {
              case e: Throwable => log.warn("onEvictEntry failed", e)
            }
            null
          }
          case retainedSegment => pointer
        }
      })
    }
  }

  def stats(details: Boolean): Seq[String] = {
    reader.lock
    try {
      Seq((s"LOGHASHMAP(${currentSegment}/${numSegments}): index.size = ${index.size} seg.entires = ${size} " +
        s"total.capacity = ${capacityInBytes / 1024 / 1024} Mb " +
        s"current.memory = ${totalSizeInBytes / 1024 / 1024} Mb " +
        s"(of that index: ${index.sizeInBytes / 1024 / 1024} Mb with load factor ${index.load}})" +
        s", compression = ${compressRatio} ")) ++
        (if (details) {
          segmentIndex.asScala.reverse.map(s => segments.get(s).stats(s)) ++
            segments.asScala.filter(segment => !segmentIndex.asScala.exists(i => segments.get(i) == segment))
              .map(s => s.stats(-1))
        } else {
          Seq()
        })
    } finally {
      reader.unlock
    }
  }

  /**
   * onEvictEntry - for applications to override to get notified about any key evicted from the tail during recycling
   * @param key sliced buffer pointing to the key in the underlying hash table. the key should not be referenced
   *            outside call to onEvictEntry as they key ByteBuffer may be reused be the wrapping iterator
   */
  def onEvictEntry(key: ByteBuffer) = {}


  //  def applyCompression(fraction: Double): Unit = {
  //    val sizeThreshold = (maxSizeInBytes * (1.0 - fraction)).toInt
  //
  //    reader.lock
  //    try {
  //      var cumulativeSize = 0
  //      segmentIndex.asScala.map(segments.get(_)).foreach(segment => {
  //        cumulativeSize += segment.usedBytes
  //        if (cumulativeSize > sizeThreshold) {
  ////          segment.compress
  //        }
  //      })
  //    } finally {
  //      reader.unlock
  //    }
  //  }

}

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

package io.amient.utils.logmap

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import io.amient.utils.AtomicInt
import net.jpountz.lz4.LZ4Factory
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage with transparent lz4 compression
 */

class SegmentDirectMemoryLZ4(capacityMb: Int, compressMinBlockSize: Int) extends Segment {

  private val log = LoggerFactory.getLogger(classOf[SegmentDirectMemoryLZ4])

  implicit def integerToAtomicInt(i: AtomicInteger) = new AtomicInt(i)

  override val capacity = capacityMb * 1024 * 1024

  private[logmap] val index = new IntIndex(capacityMb * 8192)

  private val memory = ByteBuffer.allocateDirect(capacity)

  private val lock = new ReentrantReadWriteLock

  private val compressor = LZ4Factory.fastestInstance.highCompressor

  private val decompressor = LZ4Factory.fastestInstance.fastDecompressor

  @volatile private var uncompressedSizeInBytes: Int = 0

  @volatile private var compactableMemory: Int = 0

  private val maxBlockSize = new AtomicInteger(0)

  @volatile private var valid = true

  @volatile private var counter: Int = 0

  override def size: Int = counter

  private def loadFactor: Double = usedBytes.toDouble / totalSizeInBytes

  override def totalSizeInBytes = capacity + index.capacityInBytes // + lz4Buffer.sizeInBytes.get

  override def usedBytes = memory.position + index.sizeInBytes // + lz4Buffer.sizeInBytes.get

  override def compressRatio: Double = uncompressedSizeInBytes match {
    case 0 => 1.0
    case x => usedBytes * 1.0 / (x + index.sizeInBytes) //+ lz4Buffer.sizeInBytes.get)
  }

  override def compactFactor: Double = compactableMemory.toDouble / (capacity - memory.position)

  private[logmap] val lz4Buffer = new ThreadLocal[ByteBuffer] {
    val sizeInBytes = new AtomicInteger(0)

    override def initialValue = {
      val result = ByteBuffer.allocateDirect(
        math.max(compressMinBlockSize, compressor.maxCompressedLength(maxBlockSize.get)))
      sizeInBytes.addAndGet(result.capacity)
      result
    }

    override def get = super.get match {
      case b if (b.capacity < maxBlockSize.get) => {
        sizeInBytes.addAndGet(-b.capacity)
        val n = initialValue
        set(n)
        n
      }
      case e => e
    }
  }

  override def alloc(length: Int): Int = {
    var result: Int = -1
    lock.writeLock.lock
    try {
      if (memory.remaining >= length + 5) {
        val pointer = memory.position
        memory.position(pointer + length + 5)
        memory.put(pointer, 0)
        memory.putInt(pointer + 1, length)
        result = index.put(pointer)
        counter += 1
        uncompressedSizeInBytes += (length + 5)
        maxBlockSize.setIfGreater(length)
      }
    } finally {
      lock.writeLock.unlock
    }
    result
  }

  override private[logmap] def setUnsafe(block: Int, value: ByteBuffer) = {
    val pointer = index.get(block)
    var destOffset = pointer + 5
    if (value != null) {
      var srcPos = value.position
      while (srcPos < value.limit) {
        memory.put(destOffset, value.get(srcPos))
        destOffset += 1
        srcPos += 1
      }
    }
  }

  override def put(position: Int, value: ByteBuffer): Int = {
    var resultPosition = -1
    lock.writeLock.lock
    try {
      val length = if (value == null) 0 else value.remaining
      val existingLength = sizeOf(position)
      val pointer = if (position >= 0 && length == existingLength) {
        //replace value exactly
        index.get(position)
      } else {
        //append or replace with a value of different length
        if (memory.position + length + 5 > memory.capacity) {
          return -1
        } else if (position >= 0) {
          compactableMemory += (existingLength + 5)
        } else {
          counter += 1
        }
        uncompressedSizeInBytes += (length + 5)
        maxBlockSize.setIfGreater(length)
        val putPosition = memory.position
        memory.position(memory.position + length + 5)
        putPosition
      }

      resultPosition = index.put(pointer, position)
      memory.put(pointer, 0)
      memory.putInt(pointer + 1, length)
      setUnsafe(resultPosition, value)

    } finally {
      lock.writeLock.unlock
    }
    resultPosition
  }

  override def sizeOf(block: Int): Int = {
    lock.readLock.lock
    try {
      if (block < 0) {
        return 0
      }
      val pointer = index.get(block)
      if (pointer < 0) {
        return 0
      }
      memory.get(pointer) match {
        case 0 => memory.getInt(pointer + 1)
        case 2 => {
          val nestedIndexSize = memory.getInt(pointer + 9)
          (0 to nestedIndexSize - 1).foreach(nb => {
            if (block == memory.getInt(pointer + 13 + nb * 12)) {
              return memory.getInt(pointer + 13 + nb * 12 + 8)
            }
          })
          return 0
        }
      }
    } finally {
      lock.readLock.unlock
    }
  }


  override def remove(position: Int): Unit = {
    lock.writeLock.lock
    try {
      compactableMemory += (sizeOf(position) + 5)
      counter -= 1
      index.put(-1, position)
      if (size == 0) {
        recycle
      }
    } finally {
      lock.writeLock.unlock
    }
  }

  /**
   * get - We're using decoder so that we can use the memory safely without copying it
   *
   * @param block
   * @param decoder
   * @tparam X
   * @return
   */
  override def get[X](block: Int, decoder: (ByteBuffer => X)): X = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    lock.readLock.lock
    try {
      val pointer = index.get(block)
      if (pointer < 0) {
        null.asInstanceOf[X]
      } else {
        val sliceBuffer = memory.duplicate()
        sliceBuffer.position(pointer)
        val blockType = sliceBuffer.get
        val blockLength = sliceBuffer.getInt
        sliceBuffer.limit(sliceBuffer.position + blockLength)
        val blockBuffer: ByteBuffer = blockType match {
          case 0 => sliceBuffer.slice
          case 2 => {
            val uncompressedLength = sliceBuffer.getInt
            if (log.isTraceEnabled) {
              log.trace(s"accessing block ${block} in compressed group @ ${pointer}, " +
                s"compressed = ${blockLength}, uncompressed = ${uncompressedLength}")
            }
            val nestedIndexSize = sliceBuffer.getInt
            val nestedIndex: Map[Int, (Int, Int)] = (0 to nestedIndexSize - 1).map { nb => {
              (sliceBuffer.getInt ->(sliceBuffer.getInt, sliceBuffer.getInt))
            }
            }.toMap
            val buffer = lz4Buffer.get
            buffer.clear
            decompressor.decompress(sliceBuffer, sliceBuffer.position, buffer, buffer.position, uncompressedLength)
            buffer.limit(uncompressedLength)
            val nestedOffset = nestedIndex(block)._1
            buffer.limit(nestedOffset + buffer.getInt(nestedOffset + 1) + 5)
            buffer.position(nestedOffset + 5)
            buffer.slice
          }
        }
        if (blockBuffer.remaining == 0) null.asInstanceOf[X] else decoder(blockBuffer)
      }
    } finally {
      lock.readLock.unlock
    }
  }

  override def recycle: Unit = {
    lock.writeLock.lock
    try {
      valid = true
      counter = 0
      uncompressedSizeInBytes = 0
      maxBlockSize.set(0)
      compactableMemory = 0
      index.clear
      memory.clear
    } finally {
      lock.writeLock.unlock
    }
  }

  override def compact(minCompactToFreeFactor: Double): Boolean = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    lock.writeLock.lock
    try {
      if (minCompactToFreeFactor > 0) {
        if (compactFactor < minCompactToFreeFactor || compactableMemory.toDouble / capacity < 0.01) {
          return false
        }
      }
      val compactIndex = (0 to index.count - 1).filter(index.get(_) >= 0)
      val compactedSize = compactIndex.map(b => index.get(b)).distinct.map(pointer => memory.getInt(pointer + 1) + 5).sum

      if (compactedSize == memory.position) {
        return false
      } else if (log.isDebugEnabled) {
        log.debug(s"Compacting ${compactIndex.size} blocks to from ${memory.position} to ${compactedSize} bytes")
      }
      if (compactedSize < 0) {
        throw new IllegalStateException("Segment memory is corrupt!")
      }
      try {
        val sortedBlockIndex = compactIndex.map(b => (index.get(b), b)).sorted
        uncompressedSizeInBytes = 0
        compactableMemory = 0
        maxBlockSize.set(0)
        counter = 0
        memory.clear
        var prevPointer = -1
        var prevPointerMoved = -1
        sortedBlockIndex.foreach { case (pointer, b) => {
          try {
            if (prevPointer > -1 && pointer < prevPointer) {
              throw new IllegalStateException("Illegal block pointer ordering")
            }
            if (pointer == prevPointer) {
              index.put(prevPointerMoved, b)
            } else {
              prevPointer = pointer
              prevPointerMoved = memory.position

              val blockType = memory.get(pointer)
              val blockLength = memory.getInt(pointer + 1)
              val wrappedLength = blockLength + 5
              val uncompressedLength = blockType match {
                case 0 => wrappedLength
                case 2 => memory.getInt(pointer + 5) + 9
              }

              if (pointer == memory.position) {
                memory.position(pointer + wrappedLength)
              } else if (pointer > memory.position) {
                var srcPos = pointer
                val limit = pointer + wrappedLength
                index.put(memory.position, b)
                while (srcPos < limit) {
                  memory.put(memory.get(srcPos))
                  srcPos += 1
                }
              }
              uncompressedSizeInBytes += uncompressedLength
              maxBlockSize.setIfGreater(blockLength)
            }
            counter += 1
          } catch {
            case e: Throwable => throw new Exception(s"Block ${b}, pointer = ${pointer}", e)
          }
        }
        }
        return true
      } catch {
        case e: Throwable => {
          valid = false
          throw e
        }
      }
    } finally {
      lock.writeLock.unlock
    }
  }

//  override def compress: Boolean = {
//    lock.writeLock.lock
//    try {
//      compact(0) // first normal compaction to make order memory blocks into consecutive chunks
//      val consecutiveBlockIndex = (0 to index.count - 1).filter(index.get(_) >= 0).map(b => (index.get(b), b)).sorted
//      var start = -1
//      var uncompressedBlockLen = 0
//      var end = -1
//      consecutiveBlockIndex.foreach { case (pointer, b) =>
//        val blockType = memory.get(pointer)
//        val blockLength = memory.getInt(pointer + 1)
//        val wrappedLength = blockLength + 5
//        println(s"${b} -> ${pointer} .. wrap.length ${wrappedLength}")
//        if (start >=0 && index.get(start) + uncompressedBlockLen != pointer) {
//          throw new IllegalStateException(s"Non-consecutive block memory ordering, expected block pointer ${start} -> ${index.get(start)} + ${uncompressedBlockLen} = ${index.get(start) + uncompressedBlockLen}, got ${pointer}")
//        }
//        if (blockType == 0) {
//          if (start == -1) start = b
//          end = b
//          uncompressedBlockLen += wrappedLength
//        }
//
//        if (blockType != 0 || b == index.count - 1 || uncompressedBlockLen >= compressMinBlockSize) {
//          if (start >= 0) {
//            println(s"${b} type {$blockType} triggering compression of blocks[${start} .. ${end}] with total uncompressed length ${uncompressedBlockLen}")
//            try {
//              if (uncompressedBlockLen > compressMinBlockSize * 10) {
//                throw new ArrayIndexOutOfBoundsException
//              }
//
//              val pointer = index.get(start)
//              println(s"compression source memory [${start} -> ${pointer} .. ${end} -> ${pointer + uncompressedBlockLen}]")
//              val nestedIndex = (start to end).map(b => (b, index.get(b) - pointer, memory.getInt(index.get(b) + 1)))
//              val nestedIndexSize = 4 + nestedIndex.size * 12
//              maxBlockSize.setIfGreater(9 + nestedIndexSize + compressor.maxCompressedLength(uncompressedBlockLen))
//              val buffer = lz4Buffer.get
//
//              buffer.clear
//              buffer.put(2.toByte) //block type 2 - grouped blocks
//              buffer.putInt(0) //reserved for compressed block length
//              buffer.putInt(uncompressedBlockLen) //total uncompressed len
//              buffer.putInt(nestedIndex.size) //nested index structure for retrieving the blocks after decompression
//              nestedIndex.foreach { case (b, nestedPointer, nestedBlockSize) => {
//                buffer.putInt(b)
//                buffer.putInt(nestedPointer)
//                buffer.putInt(nestedBlockSize)
//              }
//              }
//
//              val compressedLen = compressor.compress(
//                memory, pointer, uncompressedBlockLen, buffer, buffer.position, uncompressedBlockLen)
//
//              val limit = 9 + nestedIndexSize + compressedLen
//
//              println(s"compress buffer.capactity = ${buffer.capacity}, index = ${nestedIndexSize}, compressedLen = ${compressedLen}, buffer.limit = ${limit}")
//
//              if (limit < uncompressedBlockLen) {
//                buffer.putInt(1, 4 + nestedIndexSize + compressedLen)
//                var offset = 0
//
//                while (offset < limit) {
//                  memory.put(pointer + offset, buffer.get(offset))
//                  offset += 1
//                }
//                for (m <- (start to end)) {
//                  index.put(pointer, m)
//                }
//                //                if (log.isTraceEnabled) {
//                //                  log.trace
//                println(s"@${pointer} compression of blocks [${start}..${end}] of ${uncompressedBlockLen} bytes to pointer [${pointer}..${pointer + limit - 1}}]" +
//                  s" to ${memory.getInt(pointer + 1)} bytes; rate ${compressedLen * 100.0 / uncompressedBlockLen} %")
//                //                }
//
//                start = -1
//                uncompressedBlockLen = 0
//
//              }
//            } catch {
//              case e: ArrayIndexOutOfBoundsException => {
//                println(s"block ${b} -> ${pointer} of type ${blockType} = wrap.length = ${wrappedLength}")
//                System.exit(5)
//              }
//            }
//          }
//        }
//      }
//      compact(0)
//      return true
//    } finally {
//      lock.writeLock.unlock
//    }
//  }

  override def stats(s: Short): String = {
    s"SEGMENT[${s}] num.entries = ${size}, total.size = ${totalSizeInBytes / 1024 / 1024} Mb " +
      s" (INDEX size = ${index.count}, capacity = ${index.capacityInBytes / 1024 / 1024} Mb)" +
      s", load.factor = ${loadFactor}},  compact.factor = ${compactFactor}" +
      s", compression = ${compressRatio * 100.0} %"
  }

}

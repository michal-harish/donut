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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import net.jpountz.lz4.LZ4Factory
import org.apache.donut.utils.AtomicInt
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage with append-only memory and transparent lz4 compression
 */

class SegmentDirectMemoryLZ4(capacityMb: Int, compressMinBlockSize: Int) extends Segment {

  private val log = LoggerFactory.getLogger(classOf[SegmentDirectMemoryLZ4])

  implicit def integerToAtomicInt(i: AtomicInteger) = new AtomicInt(i)

  private val capacity = capacityMb * 1024 * 1024

  private[logmap] val index = new IntIndex(capacityMb * 8192)

  private val memory = {
    ByteBuffer.allocateDirect(capacity)
  }

  private val compactionLock = new ReentrantReadWriteLock

  private val compressor = LZ4Factory.fastestInstance.highCompressor

  private val decompressor = LZ4Factory.fastestInstance.fastDecompressor

  private val uncompressedSizeInBytes = new AtomicLong(0)

  private val maxBlockSize = new AtomicInteger(0)

  @volatile private var valid = true

  @volatile private var counter = 0

  override def size: Int = counter

  override def capacityInBytes = capacity + index.capacityInBytes + lz4Buffer.sizeInBytes.get

  override def sizeInBytes = memory.position + index.sizeInBytes + lz4Buffer.sizeInBytes.get

  override def compressRatio: Double = uncompressedSizeInBytes.get match {
    case 0 => 1.0
    case x => sizeInBytes * 1.0 / (x + index.sizeInBytes + lz4Buffer.sizeInBytes.get)
  }

  private[logmap] val lz4Buffer = new ThreadLocal[ByteBuffer] {
    val sizeInBytes = new AtomicInteger(0)

    //TODO custom class of ByteBuffer that can remember which block is it pointing to
    override def initialValue = {
      val result = ByteBuffer.allocateDirect(compressor.maxCompressedLength(maxBlockSize.get))
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
    compactionLock.readLock.lock
    try {
      synchronized {
        if (memory.remaining >= length + 5) {
          val pointer = memory.position
          memory.position(pointer + length + 5)
          memory.put(pointer, 0)
          memory.putInt(pointer + 1, length)
          result = index.put(pointer)
          counter += 1
          uncompressedSizeInBytes addAndGet (length + 5)
          maxBlockSize.setIfGreater(length)
        }
      }
    } finally {
      compactionLock.readLock.unlock
    }
    result
  }

  override def set(block: Int, value: ByteBuffer) = {
    compactionLock.readLock.lock
    try {
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
    } finally {
      compactionLock.readLock.unlock
    }
  }

  override def put(position: Int, value: ByteBuffer): Int = {
    var resultPosition = -1
    compactionLock.readLock.lock
    try {
      val length = value.remaining
      synchronized {
        if (memory.remaining >= length + 5) {
          val pointer = memory.position
          resultPosition = index.put(pointer, position)
          if (resultPosition >= 0) {
            memory.position(pointer + length + 5)
            memory.put(pointer, 0)
            memory.putInt(pointer + 1, length)
            set(resultPosition, value)
            counter += 1
            uncompressedSizeInBytes addAndGet (length + 5)
            maxBlockSize.setIfGreater(length)
          }
        }
      }
    } finally {
      compactionLock.readLock.unlock
    }
    resultPosition
  }

  override def sizeOf(block: Int): Int = {
    compactionLock.readLock.lock
    try {
      val pointer = index.get(block)
      if (pointer < 0) {
        -1
      } else memory.get(pointer) match {
        case 0 => memory.getInt(pointer + 1)
        case 2 => {
          val compressedGroupSize = memory.getInt(pointer + 1)
          val uncompressedGroupSize = memory.getInt(pointer + 5)
          val nestedIndexSize = memory.getInt(pointer + 9)
          (0 to nestedIndexSize - 1).foreach (nb => {
            if (block == memory.getInt(pointer + 13 + nb * 12)) {
              return memory.getInt(pointer + 13 + nb * 12 + 8)
            }
          })
          return -1
        }
      }
    } finally {
      compactionLock.readLock.unlock
    }
  }



  override def remove(position: Int): Unit = {
    compactionLock.readLock.lock
    try {
      synchronized {
        index.put(-1, position)
        counter -= 1
        if (size == 0) {
          compactionLock.readLock.unlock
          compactionLock.writeLock.lock
          try {
            recycle
          } finally {
            compactionLock.readLock.lock
            compactionLock.writeLock.unlock
          }
        }
      }
    } finally {
      compactionLock.readLock.unlock
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
    compactionLock.readLock.lock
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
            //            if (log.isTraceEnabled) {
//            log.info(s"accessing block ${block} in compressed group @ ${pointer}, compressed = ${blockLength}, uncompressed = ${uncompressedLength}")
            //            }
            val nestedIndexSize = sliceBuffer.getInt
            val nestedIndex: Map[Int, (Int, Int)] = (0 to nestedIndexSize - 1).map { nb => {
              (sliceBuffer.getInt -> (sliceBuffer.getInt, sliceBuffer.getInt))
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
      compactionLock.readLock.unlock
    }
  }

  override def recycle: Unit = {
    compactionLock.writeLock.lock
    try {
      synchronized {
        log.debug(s"Recycling segment with capacity ${memory.capacity} bytes")
        valid = true
        uncompressedSizeInBytes.set(0)
        maxBlockSize.set(0)
        index.clear
        memory.clear
      }
    } finally {
      compactionLock.writeLock.unlock
    }
  }

  override def compact: Boolean = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    compactionLock.writeLock.lock
    try {
      val compactedBlockIndex = (0 to index.count - 1).filter(index.get(_) >= 0)
      val compactedSize = compactedBlockIndex.map(b => index.get(b)).distinct.map(pointer => memory.getInt(pointer+1) + 5).sum

      if (compactedSize == memory.position) {
        return false
      } else {
        log.debug(s"Compacting ${compactedBlockIndex.size} blocks to from ${memory.position} to ${compactedSize} bytes")
      }
      if (compactedSize < 0) {
        throw new IllegalStateException("Segment memory is corrupt!")
      }
      try {
        val sortedBlockIndex = compactedBlockIndex.map(b => (index.get(b), b)).sorted
        uncompressedSizeInBytes.set(0)
        maxBlockSize.set(0)
        counter = 0
        memory.clear
        var prevPointer = -1
        sortedBlockIndex.foreach { case (pointer, b) => {
          try {
            val blockType = memory.get(pointer)
            val blockLength = memory.getInt(pointer + 1)
            val wrappedLength = blockLength + 5
            val uncompressedLength = blockType match {
              case 0 => wrappedLength
              case 2 => memory.getInt(pointer + 5) + 9
            }
            if (pointer == prevPointer) {
              val sharedBlockPointer = memory.position - wrappedLength
              index.put(sharedBlockPointer, b)
            } else {
              prevPointer = pointer
              if (pointer == memory.position) {
                memory.position(memory.position + wrappedLength)
              } else if (pointer > memory.position) {
                var srcPos = pointer
                val limit = pointer + wrappedLength
                index.put(memory.position, b)
                while (srcPos < limit) {
                  memory.put(memory.get(srcPos))
                  srcPos += 1
                }
              }
              uncompressedSizeInBytes addAndGet (uncompressedLength)
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
      compactionLock.writeLock.unlock
    }
  }

  override def compress: Boolean = {
    compactionLock.writeLock.lock
    try {
      compact // first normal compaction to make continuous blocks
      var start = -1
      var lz4UncompressedChunkLen = 0
      var end = -1
      (0 to index.count - 1).filter(index.get(_) >= 0).foreach { b =>
        val pointer = index.get(b)
        val blockType = memory.get(pointer)
        val blockLength = memory.getInt(pointer + 1)
        val wrappedLength = blockLength + 5
        if (blockType == 0) {
          if (start == -1) start = b
          end = b
          lz4UncompressedChunkLen += wrappedLength
        }
        if (blockType != 0 || b == index.count - 1 || lz4UncompressedChunkLen >= compressMinBlockSize) {
          if (start >= 0) {
            try {
              val pointer = index.get(start)
              val nestedIndex = (start to end).map(b => (b, index.get(b) - pointer, memory.getInt(index.get(b) + 1)))
              val nestedIndexSize = 4 + nestedIndex.size * 12
              maxBlockSize.setIfGreater(9 + nestedIndexSize + compressor.maxCompressedLength(lz4UncompressedChunkLen))
              val buffer = lz4Buffer.get
              buffer.clear
              buffer.put(2.toByte) //block type 2 - grouped blocks
              buffer.putInt(0) //reserved for compressed block length
              buffer.putInt(lz4UncompressedChunkLen) //total uncompressed len
              buffer.putInt(nestedIndex.size) //nested index structure for retrieving the blocks after decompression
              nestedIndex.foreach { case (b, nestedPointer, nestedBlockSize) => {
                buffer.putInt(b)
                buffer.putInt(nestedPointer)
                buffer.putInt(nestedBlockSize)
              }
              }

              val compressedLen = compressor.compress(memory, pointer, lz4UncompressedChunkLen, buffer, buffer.position, lz4UncompressedChunkLen)
              buffer.putInt(1, 4 + nestedIndexSize + compressedLen)
              var offset = 0
              val limit = 9 + nestedIndexSize + compressedLen
              while (offset < limit) {
                memory.put(pointer + offset, buffer.get(offset))
                offset += 1
              }
              for (m <- (start to end)) {
                index.put(pointer, m)
              }
//              if (log.isTraceEnabled) {
//                log.info(s"@${pointer} compression of blocks [${start}..${end}] of total ${lz4UncompressedChunkLen} down to ${memory.getInt(pointer)} bytes at rate ${compressedLen * 100.0 / lz4UncompressedChunkLen} %")
//              }
            } finally {
              start = -1
              lz4UncompressedChunkLen = 0
            }
          }
        }
      }
      compact
      return true
    } finally {
      compactionLock.writeLock.unlock
    }
  }

//  private def uncompress(pointer: Int) = {
//
//  }

}

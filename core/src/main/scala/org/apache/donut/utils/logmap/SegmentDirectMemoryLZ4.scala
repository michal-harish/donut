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
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage with append-only memory and transparent lz4 compression
 */

class SegmentDirectMemoryLZ4(capacityMb: Int, compressMinBlockSize: Int) extends Segment {

  private val log = LoggerFactory.getLogger(classOf[SegmentDirectMemoryLZ4])
  class AtomicInt(val i: AtomicInteger) {
    def setIfGreater(newValue: Integer) = setIf(newValue, (x) => newValue > x)

    def setIf(newValue: Integer, condition: (Int) => Boolean) = {
      while (!(maxBlockSize.get match {
        case x: Int if (condition(x)) => maxBlockSize.compareAndSet(x, newValue)
        case _ => true
      })) {}
    }
  }

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

  override def remove(position: Int): Unit = {
    synchronized {
      index.put(-1, position)
      counter -= 1
      if (size == 0) recycle
    }
  }

  override def put(block: ByteBuffer, position: Int = -1): Int = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    compactionLock.readLock.lock
    try {
      val uncompressedLength = if (block == null) 0 else block.remaining
      val compress: Byte = if (block == null) -1 else if (block.remaining >= compressMinBlockSize) 1 else 0
      var result = -1
      var pointer = -1
      synchronized {
        pointer = memory.position
        val wrappedLength = 5 + (compress match {
          case -1 => 0
          case 1 => {
            val minRequiredFreeSpace = compressor.maxCompressedLength(block.remaining)
            if (memory.remaining < minRequiredFreeSpace + 9) return -1
            compressor.compress(block, block.position, block.remaining, memory, pointer + 9, minRequiredFreeSpace)
          }
          case _ => {
            if (memory.remaining < block.remaining + 9) return -1
            block.remaining
          }
        })
        memory.position(pointer + wrappedLength + 4)
        memory.putInt(pointer, wrappedLength)
        memory.put(pointer + 4, compress)
        memory.putInt(pointer + 5, uncompressedLength)

        result = index.put(pointer, position)
        if (result != position) {
          counter += 1
        }
        uncompressedSizeInBytes addAndGet (9 + uncompressedLength)
        maxBlockSize.setIfGreater(uncompressedLength)
      }

      compress match {
        case 0 => {
          var destPosition = pointer + 9
          var srcPos = block.position
          while (srcPos < block.limit) {
            memory.put(destPosition, block.get(srcPos))
            destPosition += 1
            srcPos += 1
          }
        }
        case _ => {}
      }
      result
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
        val mappedBuffer = memory.duplicate()
        try {
          mappedBuffer.position(pointer)
        } catch {
          case e: IllegalArgumentException => throw e
        }
        mappedBuffer.getInt //wrappedLength not used here
        val compressType = mappedBuffer.get
        val internalLength = mappedBuffer.getInt
        compressType match {
          case 0 => {
            mappedBuffer.limit(mappedBuffer.position + internalLength)
            val slice = mappedBuffer.slice
            if (slice.remaining == 0) null.asInstanceOf[X] else decoder(slice)
          }
          case 1 => {
            val buffer = lz4Buffer.get
            decompressor.decompress(mappedBuffer, mappedBuffer.position, buffer, buffer.position, internalLength)
            buffer.limit(buffer.position + internalLength)
            if (buffer.remaining == 0) null.asInstanceOf[X] else decoder(buffer)
          }
          case 2 => {
            val nestedIndex: Map[Int, Int] = (0 to mappedBuffer.getInt - 1).map { nb => {
              (mappedBuffer.getInt -> mappedBuffer.getInt)
            }
            }.toMap
            if (log.isTraceEnabled) {
              log.trace(s"accessing block ${block} in compressed group @ ${pointer}, uncompressed length = ${internalLength}")
            }
            val buffer = lz4Buffer.get
            buffer.clear
            decompressor.decompress(mappedBuffer, mappedBuffer.position, buffer, buffer.position, internalLength)
            val nestedOffset = nestedIndex(block) + 9
            buffer.limit(nestedOffset + buffer.getInt(nestedIndex(block) + 5))
            buffer.position(nestedOffset)
            if (buffer.remaining == 0) null.asInstanceOf[X] else decoder(buffer)
          }
        }
      }
    } finally {
      compactionLock.readLock.unlock
    }
  }

  override def compact: Boolean = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    compactionLock.writeLock.lock
    try {
      val compactedBlockIndex = (0 to index.count - 1).filter(index.get(_) >= 0)
      val compactedSize = compactedBlockIndex.map(b => index.get(b)).distinct.map(pointer => memory.getInt(pointer) + 4).sum
      if (compactedSize == memory.position) {
        return false
      } else {
        //log.debug(s"Compacting ${compactedBlockIndex.size} blocks to from ${memory.position} to ${compactedSize} bytes")
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
            val wrappedLength = memory.getInt(pointer)
            val uncompressedLength = memory.getInt(pointer + 5)
            if (pointer == prevPointer) {
              index.put(memory.position - 4 - wrappedLength, b)
            } else {
              prevPointer = pointer
              if (pointer == memory.position) {
                memory.position(memory.position + 4 + wrappedLength)
              } else if (pointer > memory.position) {
                var srcPos = pointer
                val limit = pointer + wrappedLength + 4
                index.put(memory.position, b)
                while (srcPos < limit) {
                  memory.put(memory.get(srcPos))
                  srcPos += 1
                }
              }
              uncompressedSizeInBytes addAndGet (9 + uncompressedLength)
              maxBlockSize.setIfGreater(uncompressedLength)
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
        val wrappedLength = memory.getInt(pointer)
        val compressType = memory.get(pointer + 4)
        if (compressType == 0) {
          if (start == -1) start = b
          end = b
          lz4UncompressedChunkLen += wrappedLength + 4
        }
        if (compressType != 0 || b == index.count - 1 || lz4UncompressedChunkLen >= compressMinBlockSize) {
          if (start >= 0) {
            try {
              synchronized {
                val pointer = index.get(start)
                val nestedIndex = (start to end).map(b => (b, index.get(b) - pointer))
                val nestedIndexSize = 4 + nestedIndex.size * 8
                maxBlockSize.setIfGreater(9 + nestedIndexSize + compressor.maxCompressedLength(lz4UncompressedChunkLen))
                val buffer = lz4Buffer.get
                buffer.clear
                buffer.putInt(-1) //reserved for compressedLen
                buffer.put(2.toByte) //compression type 2 - grouped blocks
                buffer.putInt(lz4UncompressedChunkLen) //total uncompressed len

                buffer.putInt(nestedIndex.size) //nested index structure for retrieving the blocks after decompression
                nestedIndex.foreach { case (b, nestedPointer) => {
                  buffer.putInt(b)
                  buffer.putInt(nestedPointer)
                }
                }

                val compressedLen = compressor.compress(memory, pointer, lz4UncompressedChunkLen, buffer, 9 + nestedIndexSize, lz4UncompressedChunkLen)
                buffer.putInt(0, compressedLen + 5 + nestedIndexSize)
                var offset = 0
                val limit = compressedLen + 9 + nestedIndexSize
                while (offset < limit) {
                  memory.put(pointer + offset, buffer.get(offset))
                  offset += 1
                }
                for (m <- (start to end)) {
                  index.put(pointer, m)
                }
                if (log.isTraceEnabled) {
                  log.trace(s"compression of blocks [${start}..${end}] of total ${lz4UncompressedChunkLen} down to ${memory.getInt(pointer)} bytes at rate ${compressedLen * 100.0 / lz4UncompressedChunkLen} %")
                }
              }
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

  override def sizeOf(block: Int): Int = {
    compactionLock.readLock.lock
    try {
      val pointer = index.get(block)
      if (pointer < 0) {
        -1
      } else {
        memory.getInt(pointer) - 5
      }
    } finally {
      compactionLock.readLock.unlock
    }
  }

  override def alloc(length: Int): Int = {
    var result: Int = -1
    compactionLock.readLock.lock
    try {
      synchronized {
        if (memory.remaining >= length + 9) {
          val pointer = memory.position
          memory.position(pointer + length + 9)
          memory.putInt(pointer, length + 5)
          memory.put(pointer + 4, 0)
          memory.putInt(pointer + 5, length)
          result = index.put(pointer)
          counter += 1
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
      var destOffset = pointer + 9
      if (value != null) {
        var srcPos = value.position
        while (srcPos < value.limit) {
          memory.put(destOffset, value.get(srcPos))
          destOffset += 1
          srcPos += 1
        }
      }
      val uncompressedLength = memory.getInt(pointer + 5)
      uncompressedSizeInBytes addAndGet (uncompressedLength + 9)
      maxBlockSize.setIfGreater(uncompressedLength)
    } finally {
      compactionLock.readLock.unlock
    }
  }

  override def copy(src: Segment, srcBlock: Int, dstBlock: Int = -1): Int = {
    compactionLock.readLock.lock
    try {
      src match {
        case s: SegmentDirectMemoryLZ4 => {
          s.compactionLock.readLock.lock
          try {
            val resultBlock = if (dstBlock == -1) {
              alloc(s.sizeOf(srcBlock))
            } else {
              dstBlock
            }
            val wrappedBlock = s.getBufferSlice(srcBlock)
            var destOffset = index.get(resultBlock)
            val uncompressedLength = wrappedBlock.getInt(wrappedBlock.position + 5)
            uncompressedSizeInBytes addAndGet (uncompressedLength + 9)
            maxBlockSize.setIfGreater(uncompressedLength)
            var srcPos = wrappedBlock.position
            while (srcPos < wrappedBlock.limit) {
              memory.put(destOffset, wrappedBlock.get(srcPos))
              destOffset += 1
              srcPos += 1
            }
            resultBlock
          } finally {
            s.compactionLock.readLock.unlock
          }
        }
        case _ => throw new IllegalArgumentException
      }
    } finally {
      compactionLock.readLock.unlock
    }
  }

  private def getBufferSlice(block: Int): ByteBuffer = {
    val pointer = index.get(block)
    if (pointer < 0) {
      null
    } else {
      val wrappedBlock = memory.duplicate()
      wrappedBlock.position(pointer)
      val wrappedLength = wrappedBlock.getInt(wrappedBlock.position)
      wrappedBlock.limit(pointer + wrappedLength + 4)
      wrappedBlock
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

}

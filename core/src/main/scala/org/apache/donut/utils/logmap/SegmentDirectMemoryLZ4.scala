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

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage with append-only memory and transparent lz4 compression
 */

class SegmentDirectMemoryLZ4(capacityMb: Int, compressMinBlockSize: Int) extends Segment {

  class AtomicInt(val i: AtomicInteger) {
    def setIf(newValue: Integer, condition: (Int) => Boolean) = {
      while (!(maxBlockSize.get match {
        case x: Int if (condition(x)) => maxBlockSize.compareAndSet(x, newValue)
        case _ => true
      })) {}
    }
  }

  implicit def integerToAtomicInt(i: AtomicInteger) = new AtomicInt(i)

  private val capacity = capacityMb * 1024 * 1024

  private val index = new IntIndex(capacityMb * 8192)

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

  override def capacityInBytes = capacity + index.capacityInBytes

  override def sizeInBytes = memory.position + index.sizeInBytes // TODO + size sum of decmopressor buffer instances

  override def compressRatio: Double = uncompressedSizeInBytes.get match {
    case 0 => 1.0
    case x => memory.position.toDouble * 1 / x
  }

  private val decompressBuffer = new ThreadLocal[ByteBuffer] {
    override def initialValue = ByteBuffer.allocate(maxBlockSize.get)

    override def get = super.get match {
      case b if (b.capacity < maxBlockSize.get) => {
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
      if (size == 0) {
        //println(s"Recycling segment with capacity ${memory.capacity} bytes")
        valid = true
        uncompressedSizeInBytes.set(0)
        maxBlockSize.set(0)
        index.clear
        memory.clear
      }
    }
  }

  override def put(block: ByteBuffer, position: Int = -1): Int = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    compactionLock.readLock.lock
    try {
      val uncompressedLength = if (block == null) 0 else block.remaining
      val compress: Byte = if (block == null) -1 else if (block.remaining >= compressMinBlockSize) 3 else 0
      var result = -1
      var pointer = -1
      synchronized {
        pointer = memory.position
        val wrappedLength = 5 + (compress match {
          case -1 => 0
          case 3 => {
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
        maxBlockSize.setIf(uncompressedLength, (x) => x < uncompressedLength)
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
          case e: IllegalArgumentException => {
            println(s"!${pointer} of ${mappedBuffer.limit}")
            throw e
          }
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
          case 3 => {
            val buffer = decompressBuffer.get
            decompressor.decompress(mappedBuffer, mappedBuffer.position, buffer, buffer.position, internalLength)
            buffer.limit(buffer.position + internalLength)
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
      val compactedSize = compactedBlockIndex.map(b => memory.getInt(index.get(b)) + 4).sum
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
        sortedBlockIndex.foreach { case (pointer, b) => {
          try {
            val wrappedLength = memory.getInt(pointer)
            val uncompressedLength = memory.getInt(pointer + 5)
            if (pointer != memory.position) {
              var srcPos = pointer
              val limit = pointer + wrappedLength + 4
              index.put(memory.position, b)
              while (srcPos < limit) {
                memory.put(memory.get(srcPos))
                srcPos += 1
              }
            }
            counter += 1
            uncompressedSizeInBytes addAndGet (9 + uncompressedLength)
            maxBlockSize.setIf(uncompressedLength, (x) => x < uncompressedLength)
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
      maxBlockSize.setIf(uncompressedLength, (x) => x < uncompressedLength)
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
            maxBlockSize.setIf(uncompressedLength, (x) => x < uncompressedLength)
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

}

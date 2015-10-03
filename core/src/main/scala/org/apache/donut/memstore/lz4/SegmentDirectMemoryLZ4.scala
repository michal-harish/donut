package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import net.jpountz.lz4.LZ4Factory

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage with append-only memory and transparent lz4 compression
 */

class SegmentDirectMemoryLZ4(val capacityMb: Int, val compressMinBlockSize: Int) extends Segment {

  private val capacity = capacityMb * 1024 * 1024

  private val index = new IntIndex(capacityMb * 8192)

  private val memory = ByteBuffer.allocateDirect(capacity)

  private val compactionLock = new ReentrantReadWriteLock

  private val compressor = LZ4Factory.fastestInstance.highCompressor

  private val decompressor = LZ4Factory.fastestInstance.fastDecompressor

  private val uncompressedSizeInBytes = new AtomicLong(0)

  @volatile private var valid = true

  @volatile private var counter = 0

  override def count: Int = counter


  override def capacityInBytes = capacity + index.capacityInBytes

  override def sizeInBytes = memory.position + index.sizeInBytes

  override def compressRatio: Double = uncompressedSizeInBytes.get match {
    case 0 => 100.0
    case x => memory.position.toDouble * 100 / x
  }

  override def remove(position: Int): Unit = {
    synchronized {
      index.put(-1, position)
      counter -= 1
    }
  }

  override def put(block: ByteBuffer, position: Int = -1): Int = {
    if (!valid) throw new IllegalStateException("Segment is invalid")
    compactionLock.readLock.lock
    try {
      val uncompressedLength = block.remaining
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
        uncompressedSizeInBytes addAndGet (9 + (if (block == null) 0 else uncompressedLength))
      }

      compress match {
        case 0 => {
          var destPosition = pointer + 9
          var srcPos = block.position
          while (srcPos < block.limit) {
            memory.put(destPosition, block.get(srcPos))
            destPosition += 1
            srcPos +=1
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
   * @param buffer may be null in which case the implementation may allocate a new ByteBuffer
   * @tparam X
   * @return
   */
  override def get[X](block: Int, decoder: (ByteBuffer => X), buffer: ByteBuffer = null): X = {
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
        val compressType = mappedBuffer.get()
        val internalLength = mappedBuffer.getInt
        compressType match {
          case 0 => {
            mappedBuffer.limit(mappedBuffer.position + internalLength)
            decoder(mappedBuffer.slice())
          }
          case 3 => {
            val output = if (buffer == null) ByteBuffer.allocate(internalLength) else buffer
            decompressor.decompress(mappedBuffer, mappedBuffer.position, output, output.position, internalLength)
            output.limit(output.position + internalLength)
            decoder(output)
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
        //log.debug(s"No compactiong required for ${compactedBlockIndex.size} blocks")
        return false
      } else {
        //log.debug(s"Compacting ${compactedBlockIndex.size} blocks to from ${memory.position} to ${compactedSize} bytes")
      }
      if (compactedSize < 0) {
        throw new IllegalStateException("Segment memory is corrupted!")
      }

      val tmp = ByteBuffer.allocate(compactedSize.toInt)
      try {
        uncompressedSizeInBytes.set(0)
        counter = 0
        compactedBlockIndex.foreach(b => {
          val pointer = index.get(b)
          val wrappedLength = memory.getInt(pointer)
          val uncompressedLength = 9 + memory.getInt(pointer + 5)
          val newPointer = tmp.position
          try {
            var srcPos = pointer
            val limit = pointer + wrappedLength + 4
            while (srcPos < limit) {
              tmp.put(memory.get(srcPos))
              srcPos += 1
            }
            index.put(newPointer, b)
            counter += 1
            uncompressedSizeInBytes addAndGet uncompressedLength
          } catch {
            case e: Throwable => throw new Exception(s"Block ${b}, pointer = ${pointer}, wrap.length = ${wrappedLength}", e)
          }
        })
        tmp.flip
        memory.clear
        var srcPos = 0
        while(srcPos < tmp.limit) {
          memory.put(tmp.get(srcPos))
          srcPos += 1
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
      var srcPos = value.position
      while (srcPos < value.limit) {
        memory.put(destOffset, value.get(srcPos))
        destOffset += 1
        srcPos += 1
      }
      uncompressedSizeInBytes addAndGet (memory.getInt(pointer + 5) + 9)
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
            uncompressedSizeInBytes addAndGet (wrappedBlock.getInt(wrappedBlock.position + 5) + 9)
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

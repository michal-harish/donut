package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock

import net.jpountz.lz4.LZ4Factory

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage with append-only memory and transparent lz4 compression
 */

class SegmentDirectMemoryLZ4(val capacityMb: Int, val lz4MinBlockSize: Int = 10240) extends Segment with Compaction {


  private val capacity = capacityMb * 1024 * 1024

  private val index = new IntIndex(capacityMb * 65535)

  private val memory = ByteBuffer.allocateDirect(capacity)

  private val compressor = LZ4Factory.fastestInstance.highCompressor

  private val decompressor = LZ4Factory.fastestInstance.fastDecompressor

  private val compactionLock = new ReentrantReadWriteLock

  private var counter = 0

  private var uncompressedSizeInBytes = 0

  @volatile private var compacting = false

  @volatile private var valid = true

  override def capacityInBytes = capacity + index.capacityInBytes

  override def sizeInBytes = memory.position + index.sizeInBytes

  override def compressRatio: Int = if (uncompressedSizeInBytes == 0) 0
  else {
    100 - memory.position * 100 / uncompressedSizeInBytes
  }

  override def count: Int = counter

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
      if (compacting) throw new IllegalStateException("Segment put entered while compacting")
      val uncompressedLength = block.remaining
      val compress: Byte = if (block == null) -1 else if (block.remaining > lz4MinBlockSize) 3 else 0
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
        uncompressedSizeInBytes += 9 + (if (block == null) 0 else uncompressedLength)
      }

      compress match {
        case 0 => {
          var destPosition = pointer + 9
          for (srcPos <- (block.position to block.limit - 1)) {
            memory.put(destPosition, block.get(srcPos))
            destPosition += 1
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
        mappedBuffer.position(pointer)
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
      if (compacting) throw new IllegalStateException("Segment already compacting")
      compacting = true
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
        uncompressedSizeInBytes = 0
        counter = 0
        compactedBlockIndex.foreach(b => {
          val pointer = index.get(b)
          val wrappedLength = memory.getInt(pointer)
          val uncompressedLength = 9 + memory.getInt(pointer + 5)
          val newPointer = tmp.position
          try {
            for (srcPos <- (pointer to pointer + wrappedLength + 4 - 1)) tmp.put(memory.get(srcPos))
            index.put(newPointer, b)
            counter += 1
            uncompressedSizeInBytes += uncompressedLength
          } catch {
            case e: Throwable => throw new Exception(s"Block ${b}, pointer = ${pointer}, wrap.length = ${wrappedLength}", e)
          }
        })
        tmp.flip
        memory.clear
        for (srcPos <- (0 to tmp.limit - 1)) memory.put(tmp.get(srcPos))
        return true
      } catch {
        case e: Throwable => {
          valid = false
          throw e
        }
      }
    } finally {
      compacting = false
      compactionLock.writeLock.unlock
    }
  }

  override def copy(src: Segment, block: Int): Int = {
    compactionLock.readLock.lock
    try {
      src match {
        case s: SegmentDirectMemoryLZ4 => {
          s.compactionLock.readLock.lock
          try {
            val wrappedBlock = s.getWrappedBlock(block)
            this.putWrappedBlock(wrappedBlock)
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

  private def getWrappedBlock(block: Int): ByteBuffer = {
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

  private def putWrappedBlock(buffer: ByteBuffer): Int = {
    val wrappedLength = buffer.getInt(buffer.position) + 4
    var destPosition = -1
    var result = -1
    synchronized {
      if (memory.position + wrappedLength >= memory.capacity) {
        return -1
      } else {
        destPosition = memory.position
        result = index.put(memory.position)
        memory.position(memory.position + wrappedLength)
        uncompressedSizeInBytes += 9 + buffer.getInt(buffer.position + 5)
        counter += 1
      }
    }

    for (srcPos <- (buffer.position to buffer.position + wrappedLength - 1)) {
      memory.put(destPosition, buffer.get(srcPos))
      destPosition += 1
    }

    result
  }

}

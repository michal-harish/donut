package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

import net.jpountz.lz4.LZ4Factory

/**
 * Created by mharis on 02/10/15.
 *
 * Thread-Safe Off-Heap compaction storage unit with transparent compression
 */

class SegmentDirectMemory(val capacityMb: Int) extends Segment {

  private val capacity = capacityMb * 1024 * 1024

  private val index = new IntIndex(capacityMb * 1024)

  private val memory = ByteBuffer.allocateDirect(capacity)

  private val compressor = LZ4Factory.fastestInstance.highCompressor

  private val decompressor = LZ4Factory.fastestInstance.fastDecompressor

  private var uncompressedSizeInBytes = 0

  private var counter = 0

  override def capacityInBytes = capacity + index.capacityInBytes

  override def sizeInBytes = memory.position + index.sizeInBytes

  override def compressRatio: Int = if (uncompressedSizeInBytes == 0) 0 else 100 - memory.position * 100 / uncompressedSizeInBytes

  override def count: Int = counter

  override def remove(position: Int) : Unit = {
    synchronized {
      index.put(-1, position)
      counter -= 1
    }
  }


  override def copy(src: Segment, block: Int): Int = {
    src match {
      case s:SegmentDirectMemory => {
        val wrappedBlock = s.getWrappedBlock(block)
        this.putWrappedBlock(wrappedBlock)
      }
      case _ => throw new IllegalArgumentException
    }
  }

  override def put(block: ByteBuffer, position: Int = -1): Int = {
    val compress: Byte = if (block == null) -1 else if (block.remaining > 10240) 3 else 0
    var result = -1
    var mark = -1
    synchronized {
      if (memory.remaining < 9) return -1
      mark = memory.position
      val internalLength = compress match {
        case -1 => 0
        case 3 => {
          val minRequiredFreeSpace = compressor.maxCompressedLength(block.remaining)
          if (memory.remaining < minRequiredFreeSpace + 9) return -1
          compressor.compress(block, block.position, block.remaining, memory, mark + 9, minRequiredFreeSpace)
        }
        case _ => {
          if (memory.remaining < block.remaining) return -1
          block.remaining
        }
      }
      memory.putInt(mark, internalLength + 5)
      memory.position(mark + internalLength + 9)
      result = index.put(mark, position)
      uncompressedSizeInBytes += 9 + (if (block == 0) 0 else block.remaining)
      if (position < 0) {
        counter +=1
      }
    }

    //unsynchronized section follows
    memory.put(mark + 4, compress)
    memory.putInt(mark + 5, block.remaining)
    compress match {
      case 0 => {
        var destPosition = mark + 9
        for (srcPos <- (block.position to block.limit - 1)) {
          memory.put(destPosition, block.get(srcPos))
          destPosition += 1
        }
      }
      case _ => {}
    }
    result
  }

  override def get(block: Int, buffer: ByteBuffer = null): ByteBuffer = {
    val pointer = index.get(block)
    if (pointer < 0) {
      null
    } else {
      val mappedBuffer = memory.duplicate()
      mappedBuffer.position(pointer)
      mappedBuffer.getInt //wrappedLength not used here
      val compressType = mappedBuffer.get()
      val internalLength = mappedBuffer.getInt
      compressType match {
        case 0 => {
          mappedBuffer.limit(mappedBuffer.position + internalLength)
          mappedBuffer.slice()
        }
        case 3 => {
          val output = if (buffer == null) ByteBuffer.allocate(internalLength) else buffer
          decompressor.decompress(mappedBuffer, mappedBuffer.position, output, output.position, internalLength)
          output.limit(output.position + internalLength)
          output
        }
      }
    }
  }

  def compact: Unit = synchronized {
    //TODO readLock as well at this point
    val compactedBlockIndex = (0 to index.count - 1).filter(index.get(_) >= 0)
    val compactedSize = compactedBlockIndex.map(b => memory.getInt(index.get(b)) + 4).sum
    val tmp = ByteBuffer.allocate(compactedSize)
    uncompressedSizeInBytes = 0
    counter = 0
    compactedBlockIndex.foreach(b => {
      counter +=1
      val pointer = index.get(b)
      val wrappedLength = memory.getInt(pointer)
      uncompressedSizeInBytes += 9 + memory.getInt(pointer + 5)
      for (srcPos <- (pointer to pointer + wrappedLength + 4 - 1)) {
        tmp.put(memory.get(srcPos))
      }
    })
    tmp.flip
    memory.clear
    for (srcPos <- (0 to tmp.limit - 1)) {
      memory.put(tmp.get(srcPos))
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
    synchronized {
      if (memory.position + wrappedLength >= memory.capacity) {
        return -1
      } else {
        counter +=1
        var destPosition = memory.position
        val result = index.put(memory.position)
        for (srcPos <- (buffer.position to buffer.limit - 1)) {
          memory.put(destPosition, buffer.get(srcPos))
          destPosition += 1
        }
        memory.position(destPosition)
        uncompressedSizeInBytes += 9 + buffer.getInt(buffer.position + 5)
        result
      }
    }
  }

}

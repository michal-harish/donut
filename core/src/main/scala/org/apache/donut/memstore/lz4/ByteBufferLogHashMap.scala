package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

/**
 * Created by mharis on 01/10/15.
 *
 * This is a specialised HashMap for the purpose of very efficient expiring MemStore implementaion.
 *
 * It uses log-structured block storage, where each block contains the 2 sections: one is compacted and lz4 compressed
 *
 */
class ByteBufferLogHashMap {

  //TODO closing full segments
  //TODO concurrent access

  private val segments = new java.util.LinkedList[Segment]() //off-heap
  private val index = new java.util.HashMap[ByteBuffer, (Segment, Int)] //heap

  createNewSegment

  def numSegments = segments.size

  private def currentSegment = segments.get(segments.size - 1)

  def count: Int = {
    var total = 0
    val it = segments.iterator
    while (it.hasNext) {
      total += it.next.count
    }
    total
  }

  def compressRatio: Int = {
    var total = 0
    val it = segments.iterator
    while (it.hasNext) {
      total += it.next.compressRatio
    }
    total / segments.size
  }

  def sizeInBytes: Int = {
    var total = 0
    val it = segments.iterator
    while (it.hasNext) {
      total += it.next.sizeInBytes
    }
    total
  }

  def createNewSegment = {
    segments.add(new SegmentDirectMemory(capacityMb = 2))
  }

  def compact = {
    var i = segments.size-1
    while (i >= 0) {
      val segment = segments.get(i)
      segment.compact
      if (segment.count == 0) {
        segments.remove(i)
      }
      i -= 1
    }
  }

  def put(key: ByteBuffer, value: ByteBuffer) = {
    val existing = index.get(key)
    if (existing != null && existing._1 == currentSegment) {
      currentSegment.put(value, existing._2)
    } else {
      if (existing != null) existing._1.remove(existing._2)
      val newBlock = currentSegment.put(value)
      index.put(key, (currentSegment, newBlock))
    }
  }

  def get(key: ByteBuffer, buffer: ByteBuffer = null): ByteBuffer = {
    index.get(key) match {
      case null => null
      case (segment: Segment, block: Int) if (segment == currentSegment) => {
        currentSegment.get(block, buffer)
      }
      case (segment: Segment, block: Int) if (segment != currentSegment) => {
        val newBlock = currentSegment.copy(segment, block)
        index.put(key, (currentSegment, newBlock))
        segment.remove(block)
        currentSegment.get(newBlock, buffer)
      }
    }
  }


}





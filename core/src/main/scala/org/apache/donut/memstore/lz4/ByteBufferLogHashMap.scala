package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Created by mharis on 01/10/15.
 *
 * This is a specialised HashMap for the purpose of very efficient expiring MemStore implementaion.
 *
 * It uses log-structured block storage, where each block contains the 2 sections: one is compacted and lz4 compressed
 *
 */
class ByteBufferLogHashMap(val maxSegmentSizeMb: Int) {

  //TODO closing full segments
  private val segments = new java.util.LinkedList[Segment]()

  /* FIXME index should be off-heap
   * maybe fixed maximum key size so that we can have direct buffer
   */
  private val index = new ConcurrentHashMap[ByteBuffer, (Segment, Int)]

  createNewSegment

  def numSegments = segments.size

  //TODO this iterator is not really safe yet - just a placeholder method
  private def safeIterator = segments.iterator.asScala

  def count: Int = safeIterator.map(_.count).sum

  def compressRatio: Int = safeIterator.map(_.compressRatio).sum / segments.size

  def sizeInBytes: Long = safeIterator.map(_.sizeInBytes.toLong).sum

  def createNewSegment = {
    val segment = new SegmentDirectMemoryLZ4(capacityMb = maxSegmentSizeMb)
    segments.synchronized {
      segments.add(segment)
    }
  }

  private def currentSegment = segments.get(segments.size - 1)

  def put(key: ByteBuffer, value: ByteBuffer) = {
    val existing = index.get(key)
    if (existing != null && existing._1 == currentSegment) {
      currentSegment.put(value, existing._2) match {
        case -1 => throw new Exception("Not handled")
        case any => {}
      }
    } else {
      if (existing != null) existing._1.remove(existing._2)
      currentSegment.put(value) match {
        case -1 => {
          createNewSegment
          currentSegment.put(value) match {
            case -1 => throw new IllegalArgumentException("Value is bigger than segment size")
            case newBlock2 => index.put(key, (currentSegment, newBlock2))
          }
        }
        case newBlock => index.put(key, (currentSegment, newBlock))
      }
    }
  }

  def get(key: ByteBuffer, buffer: ByteBuffer = null): ByteBuffer = {
    index.get(key) match {
      case null => null
      case (segment: Segment, block: Int) if (segment == currentSegment) => {
        currentSegment.get(block, b => b, buffer)
      }
      case (segment: Segment, block: Int) if (segment != currentSegment) => {
        currentSegment.copy(segment, block) match {
          case -1 => throw new Exception("Not handled")
          case newBlock => {
            index.put(key, (currentSegment, newBlock))
            segment.remove(block)
            currentSegment.get(newBlock, b => b, buffer)
          }
        }
      }
    }
  }

  def compact = {
    var i = segments.size-1
    while (i >= 0) {
      segments.get(i) match {
        case segmentWithCompaction: Compaction => {
          segmentWithCompaction.compact
          if (segmentWithCompaction.count == 0) {
            segments.remove(i)
          }
        }
        case _ => {}
      }
      i -= 1
    }
  }


}





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
import scala.collection.JavaConverters._

/**
 * Created by mharis on 05/10/15.
 *
 * ConcurrentSegmentCatalog is a Thread-Safe collection of Segments. Segment is a collection of indexed blocks implemented elsewhere.
 *
 */
class ConcurrentSegmentCatalog(val segmentSizeMb: Int, val compressMinBlockSize: Int, segmentAllocCallback: (Int) => Unit) extends AnyRef {

  //TODO last access timestamp associated with each segment
  //TODO background thread for compaction with some basic configuration
  /**
   * COORD is a Coordinate type, essentially a pointer a segments[Short].blocks[Int] while the Boolean is a transit flag
   * signifying that another thread is changing the coordinates of the given block - used for concurrent touch operation, see get[X]
   */
  type COORD = (Boolean, Short, Int)

  private val segments = new java.util.LinkedList[Segment]() {
    add(newSegmentInstance)
  }

  @volatile private var currentSegment: Short = 0
  
  def isInCurrentSegment(p: COORD): Boolean = p._2 == currentSegment

  def iterator: Iterator[Segment] = segments.synchronized(segments.iterator.asScala.toList).iterator

  def sizeOf(p: COORD): Int = segments.get(p._2).sizeOf(p._3)

  def getBlock[X](p: COORD, mapper: ByteBuffer => X): X = segments.get(p._2).get(p._3, mapper)

  def removeBlock(coord: COORD) = segments.get(coord._2).remove(coord._3)

  def dropFirstSegments(nSegmentsToRemove: Int): Unit = if (nSegmentsToRemove > 0) segments.synchronized {
    for(s <- 1 to nSegmentsToRemove) segments.removeFirst
  }

  private def newSegmentInstance = new SegmentDirectMemoryLZ4(segmentSizeMb, compressMinBlockSize)
  private[logmap] def createNewSegment: Short = segments.synchronized {
    for(s <- 0 to segments.size-1) {
      if (segments.get(s).size == 0) {
        currentSegment = s.toShort
        return currentSegment
      }
    }
    segmentAllocCallback(segmentSizeMb * 1024 * 1024)
    segments.add(newSegmentInstance)
    currentSegment = (segments.size - 1).toShort
    return currentSegment
  }

  def compact = {
    //TODO if there are 2 segments with load < 0.5 merge into one of them and recycle the other
    var i = segments.size - 1
    while (i >= 0) {
      val segment = segments.get(i)
      segment.compact
      i -= 1
    }
  }

  /**
   * Warning: This is a dangerous method that can lead to memory leak if not used properly.
   * @param wrappedLength
   * @return
   */
  def alloc(wrappedLength: Int): COORD = {
    currentSegment match {
      case current: Short => segments.get(current).alloc(wrappedLength) match {
        case newBlock if (newBlock >= 0) => (false, current, newBlock)
        case _ => segments.synchronized {
          currentSegment match {
            case currentChecked => segments.get(currentChecked).alloc(wrappedLength) match {
              case newBlock if (newBlock >= 0) => (false, currentChecked, newBlock)
              case _ => {
                val newSegment = createNewSegment
                segments.get(newSegment).alloc(wrappedLength) match {
                  case -1 => throw new IllegalArgumentException(s"Value of length `${wrappedLength}` is bigger than segment size " + segmentSizeMb + " Mb")
                  case newBlock => (false, newSegment, newBlock)
                }
              }
            }
          }
        }
      }
    }
  }

  def move(iSrc: COORD, iDst: COORD): Unit = {
    val srcSegment = segments.get(iSrc._2)
    val dstSegment = segments.get(iDst._2)
    dstSegment.copy(srcSegment, iSrc._3, iDst._3)
    srcSegment.remove(iSrc._3)
  }

  def dealloc(p: COORD): Unit = segments.get(p._2).remove(p._3)

  def append(key: ByteBuffer, value: ByteBuffer): COORD = {
    if (value.remaining < compressMinBlockSize) {
      //if we know that the value is not going to be compressed, using alloc will lock significantly less
      val p: COORD = alloc(value.remaining)
      try {
        segments.get(p._2).set(p._3, value)
        return p
      } catch {
        case e: Throwable => {
          dealloc(p)
          throw e
        }
      }
    } else {
      currentSegment match {
        case current: Short => segments.get(current).put(value) match {
          case newBlock if (newBlock >= 0) => (false, current, newBlock)
          case _ => segments.synchronized {
            currentSegment match {
              case currentChecked => segments.get(currentChecked).put(value) match {
                case newBlock if (newBlock >= 0) => (false, currentChecked, newBlock)
                case _ => {
                  val newSegment = createNewSegment
                  segments.get(newSegment).put(value) match {
                    case -1 => throw new IllegalArgumentException("Value is bigger than segment size " + segmentSizeMb + " Mb")
                    case newBlock => (false, newSegment, newBlock)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

}


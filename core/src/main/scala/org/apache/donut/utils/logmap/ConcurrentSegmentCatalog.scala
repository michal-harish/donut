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
class ConcurrentSegmentCatalog(val segmentSizeMb: Int, val compressMinBlockSize: Int) extends AnyRef {

  //TODO last access timestamp associated with each segment
  //TODO background thread for compaction with some basic configuration
  /**
   * COORD type is a Coordinate pointing to a block in
   */
  type COORD = (Boolean, Short, Int)


  private val segments = new java.util.LinkedList[Segment]()

  @volatile private var currentSegment: Short = - 1

  createNewSegment

  /**
   * @param i
   * @return true if the index value belongs to the latest segment
   */
  def isInLastSegment(i: COORD): Boolean = i._2 == currentSegment

  def iterator: Iterator[Segment] = segments.synchronized(segments.iterator.asScala.toList).iterator

  def sizeOf(i: COORD): Int = segments.get(i._2).sizeOf(i._3)

  def getBlock[X](i: COORD, mapper: ByteBuffer => X): X = {
    segments.get(i._2).get(i._3, mapper)
  }

  def removeBlock(coord: COORD) = segments.get(coord._2).remove(coord._3)

  private[logmap] def createNewSegment: Short = segments.synchronized {
    for(s <- 0 to segments.size-1) {
      if (segments.get(s).size == 0) { //reuse segment that was compacted-out
        currentSegment = s.toShort
        return currentSegment
      }
    }
    segments.add(new SegmentDirectMemoryLZ4(segmentSizeMb, compressMinBlockSize))
    currentSegment = (segments.size - 1).toShort
    return currentSegment
  }

  def compact = {
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

  def dealloc(i: COORD): Unit = segments.get(i._2).remove(i._3)

  def append(key: ByteBuffer, value: ByteBuffer): COORD = {
    if (value.remaining < compressMinBlockSize) {
      //if we know that the value is not going to be compressed, using alloc will lock significantly less
      val newIndexValue = alloc(value.remaining)
      try {
        segments.get(newIndexValue._2).set(newIndexValue._3, value)
      } catch {
        case e: Throwable => {
          dealloc(newIndexValue)
          throw e
        }
      }
      newIndexValue
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


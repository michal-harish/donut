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

class ConcurrentSegmentCatalog(
                                val segmentSizeMb: Int,
                                val compressMinBlockSize: Int,
                                segmentAllocCallback: (Int) => Unit) extends AnyRef {

  private def newSegmentInstance = new SegmentDirectMemoryLZ4(segmentSizeMb, compressMinBlockSize)

  private val index = new java.util.ArrayList[Short]

  @volatile private var currentSegment: Short = 0

  private val segments = new java.util.ArrayList[Segment]() {
    add(newSegmentInstance)
    index.add(0)
  }

  /**
   * COORD is a Coordinate type, essentially a pointer a segments[Short].blocks[Int] while the Boolean is a transit flag
   * signifying that another thread is changing the coordinates of the given block - used for concurrent touch operation, see get[X]
   */
  type COORD = (Boolean, Short, Int)

  def isInCurrentSegment(p: COORD): Boolean = p._2 == currentSegment

  def iterator: Iterator[(Short, Segment)] = segments.synchronized(index.asScala.map(s => (s, segments.get(s)))).iterator

  def sizeOf(p: COORD): Int = segments.get(p._2).sizeOf(p._3)

  def getBlock[X](p: COORD, mapper: ByteBuffer => X): X = segments.get(p._2).get(p._3, mapper)

  def allocatedSizeInBytes: Long = segments.synchronized(index.asScala.map(s => segments.get(s).totalSizeInBytes.toLong)).sum

  def markForDeletion(coord: COORD) = segments.get(coord._2).remove(coord._3)

  private[logmap] def allocSegment: Short = segments.synchronized {
    segmentAllocCallback(segmentSizeMb * 1024 * 1024)

    for (s <- 0 to segments.size - 1) {
      if (segments.get(s).size == 0) {
        currentSegment = s.toShort
        index.remove(currentSegment.asInstanceOf[Object])
        index.add(currentSegment)
        return currentSegment
      }
    }

    segments.add(newSegmentInstance)
    currentSegment = (segments.size - 1).toShort
    index.add(currentSegment)
    return currentSegment
  }

  def compact = {
    for (s <- 0 to (segments.size - 1)) segments.get(s).compact
  }

  def compress(maxCapacityMb: Long, logHistoryFraction: Double) = segments.synchronized {
    val sizeThreshold = (maxCapacityMb * (1.0 - logHistoryFraction)).toInt
    var cummulativeSize = 0
    index.asScala.foreach(s => {
      val segment = segments.get(s)
      cummulativeSize += segment.usedBytes / 1024 / 1024
      if (cummulativeSize > sizeThreshold) {
        segment.compress
      }
    })
  }

  def recycleSegment(s: Short): Unit = segments.synchronized {
    if (s != currentSegment) {
      println(s"Recycling segment ${s}")
      index.remove(s.asInstanceOf[Object])
      segments.get(s).recycle
    }
  }


  /**
   * Warning: This is a dangerous method that can lead to memory leak if not used properly.
   * @param valueSize
   * @return
   */
  def alloc(valueSize: Int): COORD = {
    currentSegment match {
      case current: Short => segments.get(current).alloc(valueSize) match {
        case newBlock if (newBlock >= 0) => (false, current, newBlock)
        case _ => segments.synchronized {
          currentSegment match {
            case currentChecked => segments.get(currentChecked).alloc(valueSize) match {
              case newBlock if (newBlock >= 0) => (false, currentChecked, newBlock)
              case _ => {
                val newSegment = allocSegment
                segments.get(newSegment).alloc(valueSize) match {
                  case -1 => throw new IllegalArgumentException(s"Value of length `${valueSize}` is bigger than segment size " + segmentSizeMb + " Mb")
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
    srcSegment.get[Unit](iSrc._3, (b) => dstSegment.set(iDst._3, b))
    srcSegment.remove(iSrc._3)
  }

  def dealloc(p: COORD): Unit = segments.get(p._2).remove(p._3)

  //TODO put(key, value) and if the size of the block equals the existing size, no need to append
  def append(key: ByteBuffer, value: ByteBuffer): COORD = {
    val p: COORD = alloc(if (value == null) 0 else value.remaining)
    try {
      segments.get(p._2).set(p._3, value)
      return p
    } catch {
      case e: Throwable => {
        dealloc(p)
        throw e
      }
    }
  }

  def printStats: Unit = segments.synchronized {
    println(s"LOGHASHMAP.CATALOG, CURRENT SEGMENT = ${currentSegment}")
    index.asScala.reverse.foreach(s => segments.get(s).printStats(s))
    segments.asScala.filter(segment => !index.asScala.exists(i => segments.get(i) == segment)).foreach(s => s.printStats(-1))
  }


}


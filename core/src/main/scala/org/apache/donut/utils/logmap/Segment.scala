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

/**
 * Created by mharis on 01/10/15.
 *
 * Segment is a memory-allocation unit of the log memestore.
 *
 * All implementations of Segment must be Thread-Safe
 *
 */

trait Segment {

  def totalSizeInBytes: Int

  def usedBytes: Int

  def compact: Boolean

  def compress: Boolean

  def recycle: Unit

  /**
   * @return number of elements in the segment
   */
  def size: Int

  /**
   * Allocates a new block of storage and returns its position. The position should be filled in with `set` and indexed
   * or removed with `remove` methods.
   * @param length num bytes to allocate
   * @return index of the newly allocated block
   */
  def alloc(length: Int): Int

  /**
   * Sets the content of a block that was allocated using alloc(...)
   * @param block
   * @param value
   */
  def setUnsafe(block: Int, value: ByteBuffer)

  /**
   * @param block
   * @return length of the block as stored in the memory, in bytes
   */
  def sizeOf(block: Int): Int

  /**
   * Percentage of compressed data compared to its uncompressed size
   * @return
   */
  def compressRatio: Double = 1.0

  /**
   * @param block
   * @param decoder
   * @return
   */
  def get[X](block: Int, decoder: (ByteBuffer => X)): X

  /**
   *
   * @param block if -1 is given it behaves as append
   * @param value
   * @return the position, which is either same as the block given as argument or new block position if -1 was given
   */
  def put(block: Int, value: ByteBuffer): Int

  /**
   * Delete or mark for deletion the given position
   */
  def remove(position: Int) : Unit

  /**
   * Testing methods
   * @param block
   * @return
   */
  private[logmap] def get(block: Int): ByteBuffer = get(block, (b:ByteBuffer) => b)


  def printStats(s: Short): Unit

}

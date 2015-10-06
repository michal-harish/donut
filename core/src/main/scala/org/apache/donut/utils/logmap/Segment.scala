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

  def capacityInBytes: Int

  def sizeInBytes: Int

  def compact: Boolean

  def recycle: Unit

  /**
   * @return number of elements in the segment
   */
  def size: Int

  /**
   * @param block
   * @return wrapped length of the block as stored in the memory, in bytes
   */
  def sizeOf(block: Int): Int

  /**
   * Percentage of compressed data compared to its uncompressed size
   * @return
   */
  def compressRatio: Double = 1.0

  /**
   * Appends a block and returns it's index in the segment's block storage.
   * The block passed as argument will not be affected. Internal memory
   * position will move to the next free address and the index size will be +1
   *
   * The compression is decided based on size of the block and is transparent.
   *
   * @param block
   * @return integer index of the element's position in the segment's memory or -1 if it was not possible to append
   */
  def put(block: ByteBuffer, position: Int = -1): Int

  /**
   * Delete or mark for deletion the given position
   */
  def remove(position: Int) : Unit

  /**
   * @param block
   * @param decoder
   * @return
   */
  def get[X](block: Int, decoder: (ByteBuffer => X)): X

  def get(block: Int): ByteBuffer = get(block, (b:ByteBuffer) => b)

  /**
   * Allocates a new block of storage
   * Warning: this method can cause memory leak if the block is not consumed and index properly outside the segment
   * @param length num bytes to allocate
   * @return index of the newly allocated block
   */
  def alloc(length: Int): Int

  /**
   * Copies content of another block form source segment - the desBlock must be allocated by alloc(..)
   * @return integer index of the element's position in the segment's memory or -1 if it was not possible to append
   */
  def copy(src: Segment, srcBlock: Int, dstBlock: Int = -1): Int

  /**
   * Sets the content of a block that was allocated using alloc(...)
   * @param block
   * @param value
   */
  def set(block: Int, value: ByteBuffer)

}

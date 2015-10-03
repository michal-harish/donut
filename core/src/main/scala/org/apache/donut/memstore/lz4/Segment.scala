package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

/**
 * Created by mharis on 01/10/15.
 *
 */

trait Segment {

  def capacityInBytes: Int

  def sizeInBytes: Int

  def count: Int

  def compact: Boolean

  /**
   * @param block
   * @return wrapped length of the block as stored in the memory
   */
  def sizeOf(block: Int): Int

  /**
   * Percentage of compressed data compared to its uncompressed size
   * @return
   */
  def compressRatio: Double = 100.0

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
   * @param buffer may be null in which case the implementation may allocate a new ByteBuffer if it uses compression etc.
   * @return
   */
  def get[X](block: Int, decoder: (ByteBuffer => X), buffer: ByteBuffer = null): X

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

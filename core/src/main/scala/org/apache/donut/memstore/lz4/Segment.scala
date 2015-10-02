package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

/**
 * Created by mharis on 01/10/15.
 *
 */

trait Segment {

  def capacityInBytes: Int

  def sizeInBytes: Int

  /**
   * Proportion as percentage of how much of the memory has been saved by compression
   * @return
   */
  def compressRatio: Int = -1

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
   * Same as put but the source is another block from the source segment
   * @param src
   * @param block
   * @return integer index of the element's position in the segment's memory or -1 if it was not possible to append
   */
  def copy(src: Segment, block: Int): Int

  /**
   * Delete or mark for deletion the given position
   */
  def remove(position: Int) : Unit

  /**
   * @param block
   * @param buffer may be null in which case the implementation may allocate a new ByteBuffer
   * @return
   */
  def get(block: Int, buffer: ByteBuffer = null): ByteBuffer

  def compact: Unit

  def count: Int

}

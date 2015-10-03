package org.apache.donut.memstore.lz4

/**
 * Created by mharis on 01/10/15.
 *
 * Non-Thread-Safe structure for Int-to-Int index
 */


final class IntIndex(val growBlockSize: Int) {

  val data = new GrowableByteBuffer(growBlockSize)

  def capacityInBytes: Int = data.capacity

  def sizeInBytes: Int = data.size

  def count: Int = data.size / 4

  def put(value: Int, index: Int = -1): Int = data.putInt(index * 4, value) / 4

  def get(position: Int): Int = data.getInt(position * 4)


}

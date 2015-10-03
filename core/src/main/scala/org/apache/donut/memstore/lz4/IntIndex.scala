package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

/**
 * Created by mharis on 01/10/15.
 *
 * Non-Thread-Safe structure for Int-to-Int index
 */
final class IntIndex(val growBlockSize: Int) {
  private var data: ByteBuffer = null

  private def grow(newSize: Int) {
    val roundedNewSize = math.ceil(newSize.toDouble / growBlockSize).toInt * growBlockSize
    val newData = ByteBuffer.allocateDirect(roundedNewSize)
    if (data != null) {
      for (i <- (0 to data.capacity-1)) {
        newData.put(data.get(i))
      }
    }
    data = newData
  }

  def capacityInBytes: Int = if (data == null) 0 else data.capacity

  def sizeInBytes: Int = if (data == null) 0 else data.position

  def count: Int = if (data == null) 0 else data.position / 4

  def put(value: Int, index: Int = -1): Int = {

    if (data == null || (index == -1 && data.position + 4 > data.capacity)) {
      grow(capacityInBytes + growBlockSize)
    }
    if (index >= 0) {
      val pp = index * 4
      if (pp >= data.capacity) throw new ArrayIndexOutOfBoundsException
      data.putInt(pp, value)
      index
    } else {
      val result = data.position / 4
      data.putInt(value)
      result
    }
  }

  def get(position: Int): Int = {
    val pp = position * 4
    if (pp < 0) throw new ArrayIndexOutOfBoundsException
    if (pp >= data.capacity) throw new ArrayIndexOutOfBoundsException
    data.getInt(pp)
  }


}

package org.apache.donut.memstore.lz4

import java.nio.ByteBuffer

/**
 * Created by mharis on 03/10/15.
 * Not Thread-Safe growable byte buffer backed by direct memory
 */
class GrowableByteBuffer(val growBlockSize: Int) {

  private var data: ByteBuffer = null

  def capacity: Int = if (data == null) 0 else data.capacity

  def size: Int = if (data == null) 0 else data.position

  def available: Int = if (data == null) 0 else data.capacity - data.position

  /**
   * @param offset
   * @param value
   * @return offset of the value in the data buffer
   */
  def putInt(offset: Int, value: Int): Int = {
    if (offset < 0) {
      if (available < 4) grow
      val newOffset = data.position
      data.putInt(value)
      newOffset
    } else {
      if (offset + 4 > size) throw new ArrayIndexOutOfBoundsException
      data.putInt(offset, value)
      offset
    }
  }

  def getInt(offset: Int): Int = {
    if (offset < 0) throw new ArrayIndexOutOfBoundsException
    if (offset + 4 > size) throw new ArrayIndexOutOfBoundsException
    data.getInt(offset)
  }


  private def grow {
    val newSize = capacity + growBlockSize
    val roundedNewSize = math.ceil(newSize.toDouble / growBlockSize).toInt * growBlockSize

    val newData = ByteBuffer.allocateDirect(roundedNewSize)
    if (data != null) {
      var i = 0
      while (i < data.position) {
        newData.put(data.get(i))
        i += 1
      }
    }
    data = newData
  }
}

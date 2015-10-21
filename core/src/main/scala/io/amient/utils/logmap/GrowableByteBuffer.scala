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

package io.amient.utils.logmap

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

  def clear = data.clear

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
    if (offset < 0) throw new ArrayIndexOutOfBoundsException(s"${offset} < 0")
    if (offset + 4 > size) throw new ArrayIndexOutOfBoundsException(s"${offset} + 4 > ${size}")
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

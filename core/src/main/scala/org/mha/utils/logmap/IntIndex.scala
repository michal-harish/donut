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

package org.mha.utils.logmap

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

  def clear = data.clear

}

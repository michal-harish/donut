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

package org.apache.donut.memstore

import java.nio.ByteBuffer

/**
 * All implementation of LocalStorage must:
 * 1) Allow concurrent writes
 * 2) support nulls values
 * 3) Honor expiration limits they expose within 1 second
 */
abstract class MemStore {

  def size: Long

  def minSizeInBytes: Long

  def contains(key: Array[Byte]): Boolean

  def put(key: Array[Byte], value: Array[Byte]): Unit

  //def get(key: Array[Byte]): Option[Array[Byte]]

//  def get(key: ByteBuffer): Option[Array[Byte]] = {
//    val keyBytes = java.util.Arrays.copyOfRange(key.array, key.arrayOffset, key.arrayOffset + key.remaining)
//    get(keyBytes)
//  }
  def get[X](key: ByteBuffer, mapper: (ByteBuffer) => X) : X

  def remove(key: Array[Byte]): Option[Array[Byte]]

  def iterator: Iterator[(Array[Byte], Array[Byte])]

  def put(key: ByteBuffer, value: ByteBuffer): Unit = {
    val keyBytes = java.util.Arrays.copyOfRange(key.array, key.arrayOffset, key.arrayOffset + key.remaining)
    val valueBytes = if (value == null) null else
      java.util.Arrays.copyOfRange(value.array, value.arrayOffset, value.arrayOffset + value.remaining)
    put(keyBytes, valueBytes)
  }

  def put(key: ByteBuffer, value: Array[Byte]): Unit = {
    val keyBytes = java.util.Arrays.copyOfRange(key.array, key.arrayOffset, key.arrayOffset + key.remaining)
    put(keyBytes, value)
  }

  def contains(key: ByteBuffer): Boolean = {
    val keyBytes = java.util.Arrays.copyOfRange(key.array, key.arrayOffset, key.arrayOffset + key.remaining)
    contains(keyBytes)
  }
}

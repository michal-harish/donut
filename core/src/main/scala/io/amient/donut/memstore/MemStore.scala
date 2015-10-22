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

package io.amient.donut.memstore

import java.io.DataInputStream
import java.net.Socket
import java.nio.ByteBuffer

/**
 * All implementation of the MemStore must allow concurrent writes, support nulls values and honour expiration
 * limits they expose, at minimum the size of the store in megabytes.
 */
trait MemStore {

  def stats(details: Boolean): Seq[String]

  def size: Long

  def sizeInBytes: Long

  def compressRatio: Double

  def contains(key: ByteBuffer): Boolean

  final def get(key: ByteBuffer): Option[ByteBuffer] = get(key, b => b)

  def get[X](key: ByteBuffer, mapper: (ByteBuffer) => X): Option[X]

  def put(key: ByteBuffer, value: ByteBuffer): Unit

  def map[X](f: (ByteBuffer, ByteBuffer) => X): Iterator[X]

  final def foreach(f: (ByteBuffer, ByteBuffer) => Unit) = {
    val it = map(f)
    while (it.hasNext) it.next
  }

}

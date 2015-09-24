package org.apache.donut.memstore

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

import java.io.DataInput
import java.nio.ByteBuffer
import java.util
import java.util.Collections

/**
 * Created by mharis on 13/09/15.
 *
 * This is Thread-Safe, but in a very expensive way atm
 *
 * TODO this needs to be done such that the map is compressed in memory but constant in terms of access
 * i.e. linked structure of lz4 blocks based on access time each containing pure concurrent hashmap of values
 * with the top n blocks kept uncompressed
 */
class MemStoreDumb[V](val maxEntries: Int, serde: ((V) => Array[Byte], DataInput => V)) extends MemStore[V](serde) {
  val underlying = new util.LinkedHashMap[ByteBuffer, V]() {
    override protected def removeEldestEntry(eldest: java.util.Map.Entry[ByteBuffer, V]): Boolean = size > maxEntries
  }
  val internal = Collections.synchronizedMap(underlying)

  override def minSizeInBytes: Long = -1L

  override def size: Long = internal.size.toLong

  override def contains(key: Array[Byte]): Boolean = contains(ByteBuffer.wrap(key))

  override def contains(key: ByteBuffer): Boolean = internal.containsKey(key)

  override def put(key: Array[Byte], value: V): Unit = put(ByteBuffer.wrap(key), value)

  override def get(key: Array[Byte]): Option[V] = get(ByteBuffer.wrap(key))

  override def put(key: ByteBuffer, value: V): Unit = {
    if (value == null) {
      internal.put(key, null.asInstanceOf[V])
    } else {
      internal.put(key, value)
    }
  }

  override def get(key: ByteBuffer): Option[V] = {
    internal.containsKey(key) match {
      case false => None
      case true => {
        val value = internal.remove(key)
        internal.put(key, value)
        Some(value)
      }
    }
  }


}

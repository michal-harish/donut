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

import java.nio.ByteBuffer

import io.amient.utils.logmap.ConcurrentLogHashMap

/**
 * Created by mharis on 05/10/15.
 */
class MemStoreLogMap(val map: ConcurrentLogHashMap) extends MemStore {

  override def stats(details: Boolean): Seq[String] = map.stats(details)

  override def size: Long = map.size

  override def sizeInBytes: Long = map.totalSizeInBytes

  override def compressRatio: Double = map.compressRatio

  override def contains(key: ByteBuffer): Boolean = map.contains(key)

  override def get[X](key: ByteBuffer, mapper: (ByteBuffer) => X): Option[X] = {
    map.get(key, mapper) match {
      case null => map.contains(key) match {
        case true => Some(null.asInstanceOf[X])
        case false => None
      }
      case x => Some(x)
    }
  }

  override def touch[X](key: ByteBuffer, mapper: (ByteBuffer) => X): Option[X] = {
    map.touch(key, mapper) match {
      case null => map.contains(key) match {
        case true => Some(null.asInstanceOf[X])
        case false => None
      }
      case x => Some(x)
    }
  }

  override def put(key: ByteBuffer, value: ByteBuffer): Unit = map.put(key, value)

  override def map[X](f: (ByteBuffer, ByteBuffer) => X): Iterator[X] = map.map(f)

}

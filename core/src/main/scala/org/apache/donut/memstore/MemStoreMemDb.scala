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

import java.util.concurrent.TimeUnit
import org.mapdb._

class MemStoreMemDb(val maxSizeInMb: Int) extends MemStore {

  private val db = DBMaker
    .memoryDirectDB()
    .transactionDisable()
    //.asyncWriteEnable()
    .make()

  private val map: HTreeMap[Array[Byte], Array[Byte]] = db.hashMapCreate("DonutLocalStore")
    .expireStoreSize(maxSizeInMb.toDouble / 1024)
    .expireAfterAccess(3, TimeUnit.DAYS)// TODO this doesn't really work after bootstrap but then the store size should kick in
    .counterEnable()
    .keySerializer(Serializer.BYTE_ARRAY)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .make()

  private val store = Store.forDB(db)

  override def size: Long = map.sizeLong

  override def minSizeInBytes: Long = store.getCurrSize // bug in MapDB FreeSize and CurrSize are swapped

  override def contains(key: Array[Byte]) = {
    map.containsKey(key)
  }

  val EVICTED = Array[Byte]()

  override def put(key: Array[Byte], value: Array[Byte]): Unit = {
    if (value == null) {
      map.put(key, EVICTED)
    } else {
      map.put(key, value)
    }
  }

  override def get(key: Array[Byte]): Option[Array[Byte]] = {
    val value = map.get(key)
    value match {
      case null => None
      case v: Array[Byte] if (v.length == 0) => Some(null)
      case v => Some(v)
    }
  }

  override def remove(key: Array[Byte]): Option[Array[Byte]] = {
    val value = map.remove(key)
    value match {
      case null => None
      case v: Array[Byte] if (v.length == 0) => Some(null)
      case v => Some(value)
    }
  }

  override def iterator: Iterator[(Array[Byte], Array[Byte])] = new Iterator[(Array[Byte], Array[Byte])] {
    val it = MemStoreMemDb.this.map.entrySet.iterator

    override def hasNext: Boolean = it.hasNext

    override def next(): (Array[Byte], Array[Byte]) = {
      val entry = it.next
      (entry.getKey.array, entry.getValue)
    }
  }

}


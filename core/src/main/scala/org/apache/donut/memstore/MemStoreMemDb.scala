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
import java.util.concurrent.TimeUnit

import org.apache.donut.utils.ByteUtils
import org.mapdb._

class MemStoreMemDb(val maxSizeInMb: Int) extends MemStore {

  private val db = DBMaker
    .memoryDirectDB()
    .transactionDisable()
    .make()

  private val map: HTreeMap[Array[Byte], Array[Byte]] = db.hashMapCreate("DonutLocalStore")
    .expireStoreSize(maxSizeInMb.toDouble / 1024)
    .counterEnable()
    .keySerializer(Serializer.BYTE_ARRAY)
    .valueSerializer(Serializer.BYTE_ARRAY)
    .make()

  private val store = Store.forDB(db)

  override def size: Long = map.sizeLong

  override def sizeInBytes: Long = store.getCurrSize // bug in MapDB FreeSize and CurrSize are swapped

  override def contains(key: ByteBuffer) = {
    map.containsKey(ByteUtils.bufToArray(key))
  }

  val EVICTED = Array[Byte]()

  override def put(key: ByteBuffer, value: ByteBuffer): Unit = {
    val k = ByteUtils.bufToArray(key)
    if (value == null) {
      map.put(k, EVICTED)
    } else {
      val v = ByteUtils.bufToArray(value)
      map.put(k, v)
    }
  }

  override def get[X](key: ByteBuffer, f: (ByteBuffer) => X): Option[X] = {
    val k = ByteUtils.bufToArray(key)
    val value = map.get(k)
    value match {
      case null => None
      case v: Array[Byte] if (v.length == 0) => Some(null.asInstanceOf[X])
      case v => Some(f(ByteBuffer.wrap(v)))
    }
  }

  override def iterator: Iterator[(ByteBuffer, ByteBuffer)] = new Iterator[(ByteBuffer, ByteBuffer)] {
    val it = MemStoreMemDb.this.map.entrySet.iterator

    override def hasNext: Boolean = it.hasNext

    override def next(): (ByteBuffer, ByteBuffer) = {
      val entry = it.next
      (ByteBuffer.wrap(entry.getKey), ByteBuffer.wrap(entry.getValue))
    }
  }

  override def compressRatio: Double = 1.0

  override def applyCompression(fraction: Double): Unit = {}

  override def printStats: Unit = {
      println(s"MemDb.size = ${size}   MemDb.memory = ${sizeInBytes / 1024 / 1024} Mb")
  }
}


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

import java.io.{DataOutput, DataInput}

import org.mapdb._

class MemStoreMemDb[V](val maxSizeInMb: Int, val maxItemsCached: Int, serde: ((V) => Array[Byte], DataInput => V))
  extends MemStore[V](serde) {

  private val db = DBMaker
    .memoryDirectDB()
    .transactionDisable()
    .asyncWriteEnable()
    .cacheSize(maxItemsCached)
    .make()

  private val evicted: java.util.Set[Array[Byte]] = db.hashSetCreate("EvictedKeySet")
    .expireStoreSize(1.0)
    .serializer(Serializer.BYTE_ARRAY)
    .make()

  private val map: HTreeMap[Array[Byte], V] = db.hashMapCreate("KeyValueStore")
    .expireStoreSize(maxSizeInMb.toDouble / 1024)
    //.expireMaxSize(maxItems) //FIXME MapDB doesn't seem to honour this setting
    .counterEnable()
    .keySerializer(Serializer.BYTE_ARRAY)
    .valueSerializer(new Serializer[V]() {
    override def serialize(out: DataOutput, value: V): Unit = out.write(serde._1(value))

    override def deserialize(in: DataInput, available: Int): V = serde._2(in)

  }).make()

  private val store = Store.forDB(db)

  override def size: Long = map.sizeLong

  override def minSizeInBytes: Long = store.getCurrSize // bug in MapDB FreeSize and CurrSize are swapped

  override def contains(key: Array[Byte]) = {
    evicted.contains(key) || map.containsKey(key)
  }

  override def put(key: Array[Byte], value: V): Unit = {
    if (value == null) {
      map.remove(key)
      evicted.add(key)
    } else {
      map.put(key, value)
    }
  }

  override def get(key: Array[Byte]): Option[V] = {
    if (evicted.contains(key)) {
      Some(null.asInstanceOf[V])
    } else if (map.containsKey(key)) {
      val value = map.remove(key)
      map.put(key, value)
      Some(value)
    } else {
      None
    }
  }

}


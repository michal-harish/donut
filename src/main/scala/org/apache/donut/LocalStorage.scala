package org.apache.donut

import java.nio.ByteBuffer
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap


/**
 * Created by mharis on 13/09/15.
 *
 * This is Thread-Safe, but in a very expensive way atm
 *
 * TODO this needs to be done such that the map is compressed in memory but constant in terms of access
 * i.e. linked structure of lz4 blocks based on access time each containing pure concurrent hashmap of values
 * with the top n blocks kept uncompressed
 */
class LocalStorage[V](val maxEntries: Int) {

  val internal = Collections.synchronizedMap(new util.LinkedHashMap[ByteBuffer, V]() {
    override protected def removeEldestEntry(eldest: java.util.Map.Entry[ByteBuffer, V]): Boolean = size > maxEntries
  })

//  var head: ByteBuffer = null
//  var tail: ByteBuffer = null
//  val internal = new ConcurrentHashMap[ByteBuffer, (ByteBuffer,V)]()

  def size: Int = internal.size

  def contains(key: ByteBuffer): Boolean = internal.containsKey(key)

  def remove(key: ByteBuffer) : V = {
    internal.remove(key)
  }

  def put(key: ByteBuffer, value: V): Unit = {
    remove(key)
    internal.put(key, value)
  }

  def get(key: ByteBuffer): Option[V] = {
    if (!internal.containsKey(key)) {
      None
    } else {
      val value = remove(key)
      internal.put(key, value)
      Some(value)
    }
  }

}

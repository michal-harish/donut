package org.apache.donut

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
class LocalStorage[V](val maxEntries: Int) {

  val underlying = new util.LinkedHashMap[ByteBuffer, V]() {
    override protected def removeEldestEntry(eldest: java.util.Map.Entry[ByteBuffer, V]): Boolean = size > maxEntries
  }
  val internal = Collections.synchronizedMap(underlying)

  def size: Int = internal.size

  def put(key: String, value: V) : Unit = put(ByteBuffer.wrap(key.getBytes()), value);

  def put(key: ByteBuffer, value: V): Unit = {
    internal.remove(key)
    internal.put(key, value)
  }

  def get(key: ByteBuffer): Option[V] = {
    if (!internal.containsKey(key)) {
      None
    } else {
      val value = internal.remove(key)
      internal.put(key, value)
      Some(value)
    }
  }

}

package org.apache.donut

import java.nio.ByteBuffer
import java.util


/**
 * Created by mharis on 13/09/15.
 *
 * Not Thread-Safe
 */
class LocalStorage[V](val maxEntries: Int) {

  val internal = new util.LinkedHashMap[ByteBuffer, V]() {
    override protected def removeEldestEntry(eldest: java.util.Map.Entry[ByteBuffer, V]): Boolean = size > maxEntries
  }

  def size: Int = internal.size

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

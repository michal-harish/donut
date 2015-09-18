package org.apache.donut

import java.nio.ByteBuffer
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong


/**
 * Created by mharis on 13/09/15.
 *
 * This is Thread-Safe, but in a very expensive way atm
 *
 * TODO this needs to be done such that the map is compressed in memory but constant in terms of access
 * i.e. linked structure of lz4 blocks based on access time each containing pure concurrent hashmap of values
 * with the top n blocks kept uncompressed
 */
class LocalStorage(val maxEntries: Int) {
  val underlying = new util.LinkedHashMap[ByteBuffer, Array[Byte]]() {
    override protected def removeEldestEntry(eldest: java.util.Map.Entry[ByteBuffer, Array[Byte]]): Boolean = size > maxEntries
  }
  val internal = Collections.synchronizedMap(underlying)

  def minSizeInByte: Long = {
    val it = internal.entrySet.iterator
    var size = 0L
    while (it.hasNext) {
      val entry = it.next
      size += 32 //Map.Entry overhead
      size += entry.getKey.capacity + 16
      size += entry.getValue.length + 8
    }
   //TODO underlying hashmap capacity planning overhead
    size
  }

  //  val internal = new ConcurrentHashMap[ByteBuffer, (ByteBuffer,Array[Byte])]()

  def size: Int = internal.size

  def contains(key: ByteBuffer): Boolean = {
    internal.containsKey(key)
  }

  def put(key: ByteBuffer, value: ByteBuffer): Unit = {
    val bKey = key.slice
    internal.remove(bKey)
    if (value == null || value.remaining == 0) {
      internal.put(bKey, null)
    } else {
      val bytes = util.Arrays.copyOfRange(value.array, value.arrayOffset, value.arrayOffset + value.remaining)
      internal.put(bKey, bytes)
    }
  }

  def put(key: ByteBuffer, value: Array[Byte]): Unit = {
    val bKey = key.slice
    internal.remove(bKey)
    internal.put(bKey, value)
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    val bKey = ByteBuffer.wrap(key)
    internal.remove(bKey)
    internal.put(bKey, value)
  }

  def get(key: ByteBuffer): Option[Array[Byte]] = {
    if (!internal.containsKey(key)) {
      None
    } else {
      val value = internal.remove(key)
      internal.put(key, value)
      Some(value)
    }
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    val bKey = ByteBuffer.wrap(key)
    get(bKey)
  }

}

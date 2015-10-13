package org.apache.donut.memstore

import java.nio.ByteBuffer

import org.apache.donut.utils.logmap.ConcurrentLogHashMap

/**
 * Created by mharis on 05/10/15.
 */
class MemStoreLogMap(val maxSizeInMb: Int, val segmentSizeMb: Int, val compressMinBlockSize:Int) extends MemStore {

  val map = new ConcurrentLogHashMap(maxSizeInMb, segmentSizeMb, compressMinBlockSize, indexLoadFactor = 0.87)

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

  override def put(key: ByteBuffer, value: ByteBuffer): Unit = map.put(key, value)

  override def iterator: Iterator[(ByteBuffer, ByteBuffer)] = map.iterator

  override def printStats(details: Boolean): Unit = map.printStats(details)
}

package org.apache.donut.memstore

import org.apache.donut.utils.logmap.ConcurrentLogHashMap

/**
 * Created by mharis on 05/10/15.
 */
abstract class MemStoreLogMap(val maxSizeInMb: Int) extends MemStore {

  val map = new ConcurrentLogHashMap(maxSizeInMb, segmentSizeMb = 16)

  override def size: Long = map.size

  override def minSizeInBytes: Long = map.currentSizeInBytes

  //TODO MemStoreLogMap after refactoring MemStore interface for zero-copy

}

package net.imagini.dxp.common

import java.nio.ByteBuffer

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

/**
 * Created by mharis on 09/06/15.
 */


//class KafkaRangePartitioner(val numRegions: Int) extends Partitioner {
//
//  def this(properties: VerifiableProperties) = this()
//  private val minValue: Array[Byte] = ByteUtils.parseUUID("00000000-0000-0000-0000-000000000000")
//  private val maxValue: Array[Byte] = ByteUtils.parseUUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
//  val splitKeys: Array[Array[Byte]] = ByteUtils.split(minValue, maxValue, false, numRegions - 1).slice(0, numRegions)
//  val startKey: Array[Byte] = splitKeys(1)
//  val endKey: Array[Byte] = splitKeys(splitKeys.length-1)
//
//  override def toString: String = s"dxp.spark.RegionPartitioner(${numRegions})"
//
//  override def partition(key: Any, numPartitions: Int): Int = {
//    key match {
//      case v: Vid => findRegion(v.bytes)
//      case a: Array[Byte] if a.length > 0 => findRegion(a)
//      case b: ByteBuffer => findRegion(b.array, b.arrayOffset, b.remaining)
//      case s:String if (s.length > 0) => findRegion(ByteUtils.reverse(s.getBytes))
//      case _ => throw new IllegalArgumentException("RegionPartitioner only supports KeyValue, Vid, String and Array[Byte] keys")
//    }
//  }
//
//  /*
//   * optimised recursive quick-sort-like lookup into the splitKeys array i.e. compare the middle index
//   */
//  private def findRegion(key: Array[Byte]):Int = findRegion(key, 0, key.length)
//  private def findRegion(key: Array[Byte], keyOffset: Int, keyLength: Int): Int = findRegion(0, splitKeys.size - 1, key, keyOffset, keyLength)
//  private def findRegion(min: Int, max: Int, key: Array[Byte], keyOffset: Int, keyLength: Int): Int = {
//    if (max == min) {
//      min
//    } else if (max == min + 1) {
//      val maxKey = splitKeys(max)
//      if (ByteUtils.compare(maxKey, 0, maxKey.length, key, keyOffset, keyLength) <=0) {
//        max
//      } else {
//        min
//      }
//    } else {
//      val mid = (min + max) / 2
//      val midKey = splitKeys(mid)
//      ByteUtils.compare(midKey, 0, midKey.length, key, keyOffset, keyLength) match {
//        case c: Int if (c <= 0) => findRegion(mid, max, key, keyOffset, keyLength)
//        case c: Int if (c > 0) => findRegion(min, mid-1, key, keyOffset, keyLength)
//      }
//    }
//  }
//
//}

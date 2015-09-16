package net.imagini.dxp.common

import java.nio.ByteBuffer

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

/**
 * Created by mharis on 10/09/15.
 */
class VidPartitioner extends Partitioner {

  def this(properties: VerifiableProperties) = this()

  override def partition(key: Any, numPartitions: Int): Int = {
    val hash: Int = key match {
      case a: Array[Byte] => ByteUtils.asIntValue(a)
      case b: ByteBuffer => ByteUtils.asIntValue(b.array, b.arrayOffset)
      case v: Vid => math.abs(v.hashCode) % numPartitions
      case _ => throw new IllegalArgumentException("VidPartitioner can't partition key of type " + key.getClass)
    }
    math.abs(hash) % numPartitions
  }
}

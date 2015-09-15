package net.imagini.dxp.common

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

/**
 * Created by mharis on 10/09/15.
 */
class VidPartitioner extends Partitioner {

  def this(properties: VerifiableProperties) = this()

  override def partition(key: Any, numPartitions: Int): Int = key match {
    case a: Array[Byte] => math.abs(ByteUtils.asIntValue(a)) % numPartitions
    case vid: Vid => math.abs(vid.hashCode) % numPartitions
    case _  => throw new IllegalArgumentException("VidPartitioner can't partition key of type " + key.getClass)
  }
}

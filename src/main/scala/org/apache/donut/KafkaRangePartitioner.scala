package org.apache.donut

import java.nio.ByteBuffer

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties
import net.imagini.dxp.common.ByteUtils

/**
 * Created by mharis on 09/06/15.
 *
 * This partitioner assumes that the keyspace of byte array keys has
 * the first 4 bytes of each key symmetrically distributed across the positive int32 cardinality space
 */
class KafkaRangePartitioner extends Partitioner {

  def this(properties: VerifiableProperties) = this()

  val maxHash = BigInt(1, Array(255.toByte,255.toByte,255.toByte,255.toByte))

  override def partition(key: Any, numPartitions: Int): Int = {
    key match {
      case a: Array[Byte] if a.length > 0 => calculatePartition(numPartitions, a)
      case b: ByteBuffer => calculatePartition(numPartitions, b.array, b.arrayOffset)
      case x: Any => throw new IllegalArgumentException
    }
  }
  private def calculatePartition(numPartitions: Int, key: Array[Byte]):Int = calculatePartition(numPartitions, key, 0)

  private def calculatePartition(numPartitions: Int, key: Array[Byte], keyOffset: Int): Int = {
    val unit = maxHash / (numPartitions)
    val p = (BigInt.apply(1, key.slice(keyOffset, keyOffset + 4)) / unit).toInt
    math.min(p, numPartitions-1)
  }

}

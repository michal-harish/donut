package org.apache.donut

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties
import net.imagini.dxp.common.ByteUtils

/**
 * Created by mharis on 10/09/15.
 */
class DonutPartitioner extends Partitioner {

  def this(properties: VerifiableProperties) = this()

  override def partition(key: Any, numPartitions: Int): Int = {
    if (key.isInstanceOf[Array[Byte]]) {
      //TODO this is assuming the bytes are coming from Vid
      math.abs(ByteUtils.asIntValue(key.asInstanceOf[Array[Byte]])) % numPartitions
    } else {
      throw new IllegalArgumentException("Donut Partitioner can't partition message type " + key.getClass)
    }
  }
}

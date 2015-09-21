package org.apache.donut

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by mharis on 17/09/15.
 */
class KafkaRangePartitionerTest extends FlatSpec with Matchers {

  val p1 = new KafkaRangePartitioner()
  val numPartitions = 5

  p1.partition(Array(0.toByte,0.toByte,0.toByte,0.toByte), numPartitions) should be(0)
  p1.partition(Array(51.toByte,51.toByte,51.toByte,51.toByte), numPartitions) should be(1)
  p1.partition(Array(102.toByte,102.toByte,102.toByte,102.toByte), numPartitions) should be(2)
  p1.partition(Array(153.toByte,153.toByte,153.toByte,153.toByte), numPartitions) should be(3)
  p1.partition(Array(204.toByte,204.toByte,204.toByte,204.toByte), numPartitions) should be(4)
  p1.partition(Array(255.toByte,255.toByte,255.toByte,255.toByte), numPartitions) should be(4)
}

package org.apache.donut

import net.imagini.dxp.common.ByteUtils
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by mharis on 17/09/15.
 */
class KafkaRangePartitionerTest extends FlatSpec with Matchers {

  val p1 = new KafkaRangePartitioner()
  val numPartitions = 5

  p1.partition(ByteUtils.parseUUID("00000000-0000-0000-0000-000000000000"), numPartitions) should be(0)
  p1.partition(ByteUtils.parseUUID("33333333-3333-3333-3333-333333333333"), numPartitions) should be(1)
  p1.partition(ByteUtils.parseUUID("66666666-6666-6666-6666-666666666666"), numPartitions) should be(2)
  p1.partition(ByteUtils.parseUUID("99999999-9999-9999-9999-999999999999"), numPartitions) should be(3)
  p1.partition(ByteUtils.parseUUID("cccccccc-cccc-cccc-cccc-cccccccccccc"), numPartitions) should be(4)
  p1.partition(ByteUtils.parseUUID("ffffffff-ffff-ffff-ffff-ffffffffffff"), numPartitions) should be(4)
}

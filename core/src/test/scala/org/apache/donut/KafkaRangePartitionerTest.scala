package org.apache.donut

/**
 * Donut - Recursive Stream Processing Framework
 * Copyright (C) 2015 Michal Harish
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

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

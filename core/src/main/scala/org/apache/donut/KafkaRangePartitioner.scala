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

import java.nio.ByteBuffer

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

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
    val p = (BigInt(1, key.slice(keyOffset, keyOffset + 4)) / unit).toInt
    math.min(p, numPartitions-1)
  }

}

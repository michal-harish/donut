//package io.amient.donut
//
///**
// * Donut - Recursive Stream Processing Framework
// * Copyright (C) 2015 Michal Harish
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//import java.nio.ByteBuffer
//
//import io.amient.utils.ByteUtils
//import kafka.serializer.Encoder
//import kafka.utils.VerifiableProperties
//
///**
// * Created by mharis on 16/09/15.
// *
// * Produce message from a ByteBuffer without side effect on the given buffer.
// * Note: If the ByteBuffer key is backed by array and the entire buffer's content
// * is the remaining range of bytes, ByteUtils.bufToArray will reuse the underlying
// * array. That means that both sync and async producers may point to a volatile
// * memory which must be taken into account when implementing processors with
// * producers that use KafkaByteBufferEncoder
// */
//class KafkaByteBufferEncoder extends Encoder[ByteBuffer] {
//
//  def this(properties: VerifiableProperties) = this()
//
//  override def toBytes(t: ByteBuffer): Array[Byte] = t match {
//    case null => null
//    case b => ByteUtils.bufToArray(t)
//  }
//}

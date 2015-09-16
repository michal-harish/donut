package org.apache.donut

import java.nio.ByteBuffer

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties

/**
 * Created by mharis on 16/09/15.
 */
class ByteBufferEncoder extends Encoder[ByteBuffer] {

  def this(properties: VerifiableProperties) = this()

  override def toBytes(t: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](t.remaining)
    t.get(bytes)
    bytes
  }
}

package org.apache.donut

import java.nio.ByteBuffer

import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties

/**
 * Created by mharis on 16/09/15.
 *
 * Produce message from a ByteBuffer without side effect on the given buffer
 */
class KafkaByteBufferEncoder extends Encoder[ByteBuffer] {

  def this(properties: VerifiableProperties) = this()

  override def toBytes(t: ByteBuffer): Array[Byte] = t match {
    case null => null
    case b => {
      val p = t.position
      val bytes = new Array[Byte](t.remaining)
      t.get(bytes)
      t.position(p)
      bytes
    }
  }
}

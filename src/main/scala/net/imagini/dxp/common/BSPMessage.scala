package net.imagini.dxp.common

import java.nio.ByteBuffer

/**
 * Created by mharis on 10/09/15.
 */
object BSPMessage {

  def encodeKey(key: Vid): Array[Byte] = key.bytes

  def encodePayload(payload: (Byte, Map[Vid, Edge])): Array[Byte]  = {
    val (iter, edges) = payload
    val len = edges.foldLeft(1 + 2)((l, item) => l + 8 + 1 + item._1.bytes.length + 4)
    val result = ByteBuffer.allocate(len)
    result.put(iter)
    result.putShort(edges.size.toShort)
    edges.foreach { case (k, v) => {
      result.putLong(v.ts)
      result.put(k.bytes.length.toByte)
      result.put(k.bytes)
      result.put(v.bytes)
    }
    }
    result.array
  }

  def decodeKey(key: Array[Byte]): Vid = Vid(key)
  def decodePayload(bytes: Array[Byte]): (Byte, Map[Vid, Edge]) = decodePayload(bytes, 0)
  def decodePayload(bytes: Array[Byte], offset: Int): (Byte, Map[Vid, Edge]) = {
    val payload = ByteBuffer.wrap(bytes, offset, bytes.length - offset)
    val iter = payload.get
    val size = payload.getShort.toInt
    (iter, (for (i <- (1 to size)) yield {
      val ts = payload.getLong
      val vidBytes = new Array[Byte](payload.get())
      payload.get(vidBytes)
      val vid = Vid(vidBytes)
      val edgeBytes = new Array[Byte](4)
      payload.get(edgeBytes)
      val edge = Edge.applyVersion(edgeBytes, ts)
      (vid, edge)
    }).toMap)
  }

}


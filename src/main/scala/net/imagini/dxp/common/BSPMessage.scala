package net.imagini.dxp.common

import java.nio.ByteBuffer

/**
 * Created by mharis on 10/09/15.
 */
object BSPMessage {

  def encodeKey(key: Vid): ByteBuffer = ByteBuffer.wrap(key.bytes)

  def decodeKey(key: ByteBuffer): Vid = Vid(key.array)

  def encodePayload(payload: (Byte, Map[Vid, Edge])): ByteBuffer = {
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
    ByteBuffer.wrap(result.array)
  }

  def decodePayload(payload: ByteBuffer): (Byte, Map[Vid, Edge]) = {
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


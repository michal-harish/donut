package net.imagini.dxp.donut

import java.nio.ByteBuffer

import net.imagini.dxp.common.{Vid, Edge, ByteUtils}
import org.apache.donut.DonutMessage

/**
 * Created by mharis on 10/09/15.
 */
object GraphMessage {
  def apply(key: Vid, iter: Int, edges: Map[Vid, Edge]) = new GraphMessage(serializeKey(key), serializePayload((iter.toByte, edges)))
  def serializeKey(key: Vid): Array[Byte] = key.bytes
  def serializePayload(payload: (Byte, Map[Vid, Edge])): Array[Byte] = {
    val (iter, edges) = payload
    val len = edges.foldLeft(1 + 2)((l, item) => l + 8 + 1 + item._1.bytes.length + 4 )
    val result = ByteBuffer.allocate(len)
    result.put(iter)
    result.putShort(edges.size.toShort)
    edges.foreach{ case (k,v) => {
      result.putLong(v.ts)
      result.put(k.bytes.length.toByte)
      result.put(k.bytes)
      result.put(v.bytes)
    } }
    result.array
  }
}

class GraphMessage(key: Array[Byte], val payload: Array[Byte])
  extends DonutMessage[Vid, (Byte, Map[Vid, Edge])]("graphstream", key, payload) {

  override def decodeKey: Vid = Vid(key)

  override def decodePayload: (Byte, Map[Vid, Edge]) = {
    val buf = ByteBuffer.wrap(payload)
    val iter = buf.get
    val size = buf.getShort.toInt
    (iter, (for(i <- (1 to size)) yield {
      val ts = buf.getLong
      val vidBytes = new Array[Byte](buf.get())
      buf.get(vidBytes)
      val vid = Vid(vidBytes)
      val edgeBytes = new Array[Byte](4)
      buf.get(edgeBytes)
      val edge = Edge.applyVersion(edgeBytes, ts)
      (vid, edge)
    }).toMap)
  }

}


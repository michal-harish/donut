package io.amient.donut.memstore

import java.io.DataInputStream
import java.net.Socket
import java.nio.ByteBuffer

/**
 * Created by mharis on 22/10/15.
 */
class MemStoreClient(host: String, port: Int) {
  val k = ByteBuffer.allocate(100)
  val v = ByteBuffer.allocate(65535)
  var socket = new Socket(host, port)
  lazy val in = new DataInputStream(socket.getInputStream)

  final def close = if (socket != null) {
    socket.close
    socket = null
  }

  final def foreach(f: (ByteBuffer, ByteBuffer) => Unit) = {
    val it = map(f)
    while (it.hasNext) it.next
  }

  final def map[X](f: (ByteBuffer, ByteBuffer) => X): Iterator[X] = {
    return new Iterator[X] {

      private var current: X = null.asInstanceOf[X]
      override def hasNext: Boolean = {
        if (current != null) {
          true
        } else {
          val keyLen = in.readInt
          if (keyLen <= 0) {
            close
            false
          } else {
            in.readFully(k.array, 0, keyLen)
            val valueLen = in.readInt
            in.readFully(v.array, 0, valueLen)
            current = f(k, v)
            true
          }
        }
      }

      override def next(): X = if (current == null) {
        throw new NoSuchElementException
      } else {
        val result = current
        current = null.asInstanceOf[X]
        result
      }
    }
  }

}

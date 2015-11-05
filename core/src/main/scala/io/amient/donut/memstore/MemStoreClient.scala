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

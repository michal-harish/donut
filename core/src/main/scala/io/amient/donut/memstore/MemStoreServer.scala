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

import java.io.{BufferedOutputStream, DataOutputStream}
import java.net.ServerSocket
import java.util.concurrent.Executors

import io.amient.utils.ByteUtils
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 22/10/15.
 */
class MemStoreServer(val memstore: MemStore) {

  private val log = LoggerFactory.getLogger(classOf[MemStoreServer])

  val server = new ServerSocket(50531)

  val threadPool = Executors.newCachedThreadPool()

  def getListeningPort: Int = server.getLocalPort

  val acceptor = new Thread {
    override def run: Unit = {
      while (!isInterrupted) {
        val socket = server.accept
        log.info(s"Accepted connection from ${socket.getLocalPort}")
        threadPool.submit(new Runnable() {
          override def run(): Unit = {
            log.info(s"Sending remote scan")
            val os = socket.getOutputStream()
            //val bos = new BufferedOutputStream(os)
            val out = new DataOutputStream(os)
            try {
              memstore.foreach { case (key, value) => {
                if (value.remaining > 0) {
                  out.writeInt(key.remaining)
                  ByteUtils.bufToStream(key, out)
                  out.writeInt(value.remaining)
                  ByteUtils.bufToStream(value, out)
                }
              }
              }
              log.info("Completed remote scan")
              out.writeInt(0)
              out.flush
              os.flush
            } catch {
              case e: Throwable => e.printStackTrace
            } finally {
              socket.close()
            }
          }
        })
      }
    }
  }

  def start = acceptor.start

  def stop = acceptor.interrupt

}

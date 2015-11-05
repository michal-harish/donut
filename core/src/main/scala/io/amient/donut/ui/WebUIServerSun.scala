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

package io.amient.donut.ui

import java.io.BufferedOutputStream
import java.net.{URLDecoder, InetSocketAddress}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 19/10/15.
 */
class WebUIServerSun(host: String, port: Int) extends WebUIServer(host, port) {

  private val log = LoggerFactory.getLogger(classOf[WebUIServerSun])

  val httpd = HttpServer.create(new InetSocketAddress(host, port), 0)
  httpd.setExecutor(null)
  httpd.createContext("/", new HttpHandler() {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        val method = exchange.getRequestMethod.toUpperCase
        val uri = exchange.getRequestURI.getPath
        val params: Map[String, String] = exchange.getRequestURI.getRawQuery match {
          case null => Map()
          case rawQuery => rawQuery.split("&").map(_.split("=") match {
            case a:Array[String] if (a.length == 1) => (a(0) -> "")
            case a:Array[String] if (a.length == 2) => (a(0) -> URLDecoder.decode(a(1), "UTF-8"))
          }).toMap
        }
        val (status, mime, body) = WebUIServerSun.this.handle(method, uri, params)
        exchange.getResponseHeaders.add("Content-Type", mime)
        exchange.sendResponseHeaders(status, body.length)
        val os = new BufferedOutputStream(exchange.getResponseBody)
        os.write(body.getBytes, 0, body.length)
        os.flush
        os.close
      } catch {
        case e: Throwable => log.error("Error while handling http exchange", e)
      }
    }
  })

  override def start: Unit = httpd.start

  override def getListeningPort: Int = httpd.getAddress.getPort

  override def stop: Unit = httpd.stop(0)
}

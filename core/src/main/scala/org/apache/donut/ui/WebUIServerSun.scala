package org.apache.donut.ui

import java.io.BufferedOutputStream
import java.net.{URLDecoder, InetSocketAddress}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

/**
 * Created by mharis on 19/10/15.
 */
class WebUIServerSun(host: String, port: Int) extends WebUIServer(host, port) {

  val httpd = HttpServer.create(new InetSocketAddress(host, port), 0)
  httpd.setExecutor(null)
  httpd.createContext("/", new HttpHandler() {
    override def handle(exchange: HttpExchange): Unit = {
      val method = exchange.getRequestMethod.toUpperCase
      val uri = exchange.getRequestURI.getPath
      val params: Map[String,String] = exchange.getRequestURI.getRawQuery match {
        case null => Map()
        case rawQuery => rawQuery.split("&").map(_.split("=") match {
          case Array(name, encodedValue) => (name -> URLDecoder.decode(encodedValue))
        }).toMap
      }
      params.foreach(println)
      val (status, mime, body) = WebUIServerSun.this.handle(method, uri, params)
      exchange.getResponseHeaders.add("Content-Type", mime)
      exchange.sendResponseHeaders(status, body.length)
      val os = new BufferedOutputStream(exchange.getResponseBody)
      os.write(body.getBytes, 0, body.length)
      os.close()
    }
  })

  override def start: Unit = httpd.start

  override def getListeningPort: Int = httpd.getAddress.getPort

  override def stop: Unit = httpd.stop(0)
}

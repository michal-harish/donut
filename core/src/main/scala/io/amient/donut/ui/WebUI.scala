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

import java.net.{HttpURLConnection, URLEncoder, URL}

import io.amient.donut.metrics.{Metric, Metrics, Status}

/**
 * Created by mharis on 17/10/15.
 */
class WebUI(masterUrl: URL) extends UI {

  def this() = this(null)

  if (masterUrl != null) setServerUrl(masterUrl)

  @volatile private var server: WebUIServer = null

  override def started: Boolean = server != null

  override def getLatestProgress: Float = server match {
    case null => 0f
    case s => s.getProgress
  }

  override def startServer(host: String, port: Int): Boolean = {
    server = new WebUIServerSun(host, port)
    server.start
    url = new URL("http", host, server.getListeningPort, "/")
    started
  }

  override def stopServer: Unit = {
    if (server != null) {
      try {
        server.stop
      } finally {
        server = null
        url = null
      }
    }
  }

  def updateAttributes(attr: Map[String, Any]): Boolean = {
    post("/attribute", attr.map(x => (x._1, x._2.toString)))
  }

  override def updateMetric(partition: Int, name: String, cls: Class[_ <: Metric], value: Any, hint: String = ""): Boolean = {
    post("/metrics", Map(
        "p" -> partition.toString,
        "c" -> cls.getCanonicalName,
        "n" -> name,
        "v" -> value.toString,
        "h" -> hint))
  }


  private def post(uri: String, params: Map[String, String]): Boolean = {
    var lastError: Throwable = null
    var numRetries = 0
    while (numRetries < 5) try {
      val postUrl = new URL(url, uri + "?" +
        params.map { case (k, v) => s"${k}=${URLEncoder.encode(v, "UTF-8")}" }.mkString("&"))
      val c = postUrl.openConnection.asInstanceOf[HttpURLConnection]
      try {
        c.setRequestMethod("POST")
        c.setRequestProperty("User-Agent", "DonutApp")
        c.setRequestProperty("Accept-Language", "en-US,en;q=0.5")
        c.setRequestProperty("Content-Length", "0")
        if (c.getResponseCode == 202) {
          return true
        } else {
          throw new Exception(s"POST not accepted by the master tracker at ${postUrl}, request = ${params}" +
            s", response = ${c.getResponseCode}: ")
        }
      } finally {
        c.disconnect
      }
    } catch {
      case e: Throwable => {
        lastError = e
        numRetries += 1
        if (numRetries < 3) Thread.sleep(500)
      }
    }
    throw lastError
  }

}

package org.apache.donut.ui

import java.net.{HttpURLConnection, URLEncoder, URL}

import org.apache.donut.DonutApp
import org.apache.donut.metrics.Metric

/**
 * Created by mharis on 17/10/15.
 */
class WebUI(partition: Int, existingMasterUrl: URL) extends UI {

  def this() = this(-1, null)

  @volatile private var server: WebUIServer = null

  private var url: URL = existingMasterUrl

  override def serverUrl: URL = url

  override def started: Boolean = server != null

  override def getLatestProgress: Float = server match {
    case null => 0f
    case s => s.getProgress(partition)
  }

  override def startServer(app: DonutApp[_], host: String, port: Int): Boolean = {
    server = new WebUIServer(app, host, port)
    server.start()
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

  override def updateMetric(name: String, cls: Class[_ <: Metric], value: Any, hint: String = ""): Boolean = {
    try {
      post("/metrics", Map(
        "p" -> partition.toString,
        "c" -> cls.getCanonicalName,
        "n" -> name,
        "v" -> value.toString,
        "h" -> hint))
    } catch {
      case e: Throwable => false
    }
  }

  override def updateError(e: Throwable): Boolean = try {
    post("/errors", Map(
      "p" -> partition.toString,
      "e" -> e.getClass.getCanonicalName,
      "t" -> e.getStackTraceString
    ))
  } catch {
    case e: Throwable => false
  }

  private def post(uri: String, params: Map[String, String]): Boolean = {
    var lastError: Throwable = null
    var numRetries = 0
    while (numRetries < 3) try {
      val postUrl = new URL(url, uri + "?" +
        params.map { case (k, v) => s"${k}=${URLEncoder.encode(v, "UTF-8")}" }.mkString("&"))
      //log.debug(s"POST ${url.toString}")
      val c = postUrl.openConnection.asInstanceOf[HttpURLConnection]
      try {
        c.setRequestMethod("POST")
        c.setRequestProperty("User-Agent", "DonutApp")
        c.setRequestProperty("Accept-Language", "en-US,en;q=0.5")
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
      }
    }
    throw lastError
  }

}

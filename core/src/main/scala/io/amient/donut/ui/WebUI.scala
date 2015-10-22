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

  override def updateError(partition: Int, e: Throwable): Boolean = {
    updateMetric(partition, Metrics.LAST_ERROR, classOf[Status], e.getMessage, e.getStackTraceString)
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
      //log.debug(s"POST ${url.toString}")
      val c = postUrl.openConnection.asInstanceOf[HttpURLConnection]
      try {
        c.setRequestMethod("POST")
        c.setRequestProperty("User-Agent", "DonutApp")
        c.setRequestProperty("Accept-Language", "en-US,en;q=0.5")
        c.setRequestProperty("Content-Length", "0")
//        c.setDoOutput(true)
//        c.getOutputStream.flush
//        c.getOutputStream.close
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

package io.amient.donut.ui

import java.net.URL

import io.amient.donut.metrics.{Status, Metrics, Metric}

/**
 * Created by mharis on 17/10/15.
 */
trait UI {

  protected var url: URL = null

  final def serverUrl: URL = url

  final def setServerUrl(trackingUrl: URL) = this.url = trackingUrl

  def updateAttributes(attr: Map[String,Any]): Boolean

  final def updateError(partition: Int, e: Throwable): Boolean = {
    updateStatus(partition, Metrics.LAST_ERROR, e.getMessage, e.getStackTraceString)
  }

  final def updateStatus(partition: Int, name: String, value: String, hint: String = ""): Boolean = {
    updateMetric(partition, name, classOf[Status], value, hint)
  }

  def updateMetric(partition: Int, name: String, cls: Class[_ <: Metric], value: Any, hint: String = ""): Boolean

  def getLatestProgress: Float

  def startServer(host: String, port: Int): Boolean

  def started: Boolean

  def stopServer: Unit

}

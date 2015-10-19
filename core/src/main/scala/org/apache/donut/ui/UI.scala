package org.apache.donut.ui

import java.net.URL

import org.apache.donut.metrics.Metric

/**
 * Created by mharis on 17/10/15.
 */
trait UI {

  protected var url: URL = null

  final def serverUrl: URL = url

  final def setServerUrl(trackingUrl: URL) = this.url = trackingUrl

  def updateAttributes(attr: Map[String,Any]): Boolean

  def updateMetric(partition: Int, name: String, cls: Class[_ <: Metric], value: Any, hint: String = ""): Boolean

  def updateError(partition: Int, e: Throwable): Boolean

  def getLatestProgress: Float

  def startServer(host: String, port: Int): Boolean

  def started: Boolean

  def stopServer: Unit

}

package org.apache.donut.ui

import java.net.URL

import org.apache.donut.DonutApp
import org.apache.donut.metrics.Metric

/**
 * Created by mharis on 17/10/15.
 */
trait UI {

  def updateMetric(name: String, cls: Class[_ <: Metric], value: Any, hint: String = ""): Boolean

  def updateError(e: Throwable): Boolean

  def getLatestProgress: Float

  def startServer(app: DonutApp[_], host: String, port: Int): Boolean

  def serverUrl: URL

  def started: Boolean

  def stopServer: Unit

}

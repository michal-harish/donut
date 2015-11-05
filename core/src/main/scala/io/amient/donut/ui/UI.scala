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

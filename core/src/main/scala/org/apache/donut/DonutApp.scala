package org.apache.donut

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

import java.lang.reflect.Constructor
import java.net.{NetworkInterface, URL}
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.donut.ui.{UI, WebUI}
import org.apache.yarn1.{YarnClient, YarnContainerRequest, YarnMaster}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Created by mharis on 14/09/15.
 * The DonutApp must be extended by the application and then either runOnYarn or runLocally
 */
class DonutApp[T <: DonutAppTask](val config: Properties)(implicit t: ClassTag[T]) extends YarnMaster(config) {
  config.setProperty("yarn1.application.type", "DONUT")

  private val log = LoggerFactory.getLogger(classOf[DonutApp[_]])

  val taskClass: Class[T] = t.runtimeClass.asInstanceOf[Class[T]]
  val kafkaUtils: KafkaUtils = new KafkaUtils(config)
  val topics = config.getProperty("topics").split(",").toSeq
  val groupId = config.getProperty("group.id")
  val totalMainMemoryMb = config.getProperty("direct.memory.mb", "0").toInt
  val taskOverheadMemMb = config.getProperty("task.overhead.memory.mb", "256").toInt
  val taskJvmArgs = config.getProperty("task.jvm.args", "")
  val taskPriority: Int = config.getProperty("task.priority", "0").toInt

  def numLogicalPartitions = numPartitions

  type PARTITION_INFO = (String, Int, Long, Long, Long)

  private var numPartitions = -1
  private var lastOffsets: (Long, Seq[PARTITION_INFO]) = (-1, Seq())

  private val ui: UI = new WebUI //TODO send the ui implementing class to the tasks

  final override protected def getTrackingURL(prefHost: String = null, prefPort: Int = 0): java.net.URL = {
    if (!ui.started) {
      val host = if (prefHost != null) prefHost
      else {
        //TODO maybe the selection of best hostname should be delegated to Yarn1
        val addresses = NetworkInterface.getNetworkInterfaces.asScala.flatMap(_.getInterfaceAddresses.asScala).map(_.getAddress)
        val availableAddresses = addresses.filter(_.isSiteLocalAddress).toList
        val namedHosts = availableAddresses.filter(a => a.getHostAddress != a.getCanonicalHostName)
        if (namedHosts.size > 0) {
          namedHosts.head.getCanonicalHostName
        } else {
          availableAddresses.head.getHostAddress
        }
      }
      ui.startServer(this, host, prefPort)
    }
    ui.serverUrl
  }

  final def runOnYarn(awaitCompletion: Boolean): Unit = {
    try {
      YarnClient.submitApplicationMaster(config, this.getClass, Array[String](), awaitCompletion)
    } catch {
      case e: Throwable => {
        log.error("", e)
        System.exit(1)
      }
    }
  }

  /**
   * runLocally will create a thread for each logical partition that would be a container in the YARN context.
   * Any command received on the stdin will be given to all running tasks via executeCommand(...) method.
   *
   * Each task thread may still contain multiple fetcher threads depending on the partitioning scheme and
   * cogroup and max.tasks configuration.
   *
   * @param debugOnePartition if 'true' is passed, only one thread for logical partition 0 will be launched
   */
  final def runLocally(debugOnePartition: Int = -1): Unit = {
    try {
      numPartitions = kafkaUtils.getNumLogicalPartitions(topics)
      val trackingUrl = getTrackingURL("localhost", 8099)
      log.info("APPLICATION TRACKING URL: " + trackingUrl)

      val executor = Executors.newFixedThreadPool(numPartitions + 1)

      val taskConstructor: Constructor[T] = taskClass.getConstructor(
        classOf[Properties], classOf[URL], classOf[Int], classOf[Int], classOf[Seq[String]])

      val tasks = (if (debugOnePartition >= 0) {
        val t0 = taskConstructor.newInstance(config, trackingUrl, new Integer(debugOnePartition), new Integer(numPartitions), topics)
        executor.submit(t0)
        Seq(t0)
      } else (0 to numPartitions - 1).map(lp => {
        val t = taskConstructor.newInstance(config, trackingUrl, new Integer(lp), new Integer(numPartitions), topics)
        executor.submit(t)
        t
      }))

      executor.shutdown

      while (!executor.isTerminated) {
        if (executor.awaitTermination(30, TimeUnit.SECONDS)) return
        println(s"Progress ${getProgress * 100.0}%")
      }

    } catch {
      case e: Throwable => {
        log.error("", e)
        System.exit(1)
      }
    }

    System.exit(0)
  }

  /**
   * Yarn Master Startup hook
   */
  final override protected def onStartUp(originalArgs: Array[String]): Unit = {
    numPartitions = kafkaUtils.getNumLogicalPartitions(topics)
    val taskHeapMemMb = taskOverheadMemMb * 4 / 5
    val taskDirectMemMb = totalMainMemoryMb / numPartitions + (taskOverheadMemMb - taskHeapMemMb)
    requestContainerGroup((0 to numPartitions - 1).map(lp => {
      val args: Array[String] = Array(taskClass.getName, getTrackingURL().toString, lp.toString, numPartitions.toString) ++ topics
      new YarnContainerRequest(DonutYarnContainer.getClass, args, taskPriority, taskDirectMemMb, taskHeapMemMb, 1)
    }).toArray)
  }

  /**
   * Instead of reporting progress as ratio of completed tasks which is more suitable for finite jobs,
   * in streaming progress can be seen as an inverse of lag
   * @return average percentage representation of how the underlying fetchers are caught up with available data.
   */
  final protected override def getProgress: Float = {
    math.min(math.max(0, ui.getLatestProgress), 1)
  }

  /**
   * Yarn Master Completion hook
   */
  override protected def onCompletion: Unit = {
    ui.stopServer
  }


}


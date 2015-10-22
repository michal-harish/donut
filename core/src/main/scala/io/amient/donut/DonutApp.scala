package io.amient.donut

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

import java.util.Properties

import io.amient.donut.metrics.Metrics
import io.amient.donut.ui.{UI, WebUI}
import io.amient.utils.AddressUtils
import io.amient.yarn1.{YarnClient, YarnContainerContext, YarnContainerRequest, YarnMaster}
import org.slf4j.LoggerFactory

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

  private val ui: UI = new WebUI //TODO send the ui implementing class to the tasks

  final override protected def provideTrackingURL(prefHost: String = null, prefPort: Int = 0): java.net.URL = {
    if (!ui.started) {
      val host = AddressUtils.getLANHost(prefHost)
      ui.startServer(host, prefPort)
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
      config.setProperty("yarn1.client.tracking.url", provideTrackingURL("localhost", 8099).toString)
      config.put("yarn1.local.mode", "true")
      if (debugOnePartition >=0) {
        config.put("debug.partitions", s"${debugOnePartition}")
      }
      YarnClient.submitApplicationMaster(config, this.getClass, Array[String](), true)
  }

  private var partitionList: List[Int] = null

  /**
   * Yarn Master or Local Startup hook
   */
  final override protected def onStartUp(originalArgs: Array[String]): Unit = {
    numPartitions = kafkaUtils.getNumLogicalPartitions(topics)
    ui.setServerUrl(getTrackingURL)
    ui.updateAttributes(
      Map(
        "numLogicalPartitions" -> numPartitions,
        "appClass" -> this.getClass.getSimpleName,
        "kafkaGroupId" -> config.get("group.id"),
        "kafkaBrokers" -> kafkaUtils.kafkaBrokers,
        "topics" -> topics.mkString(","),
        "masterMemoryMb" -> masterMemoryMb,
        "masterCores" -> masterCores,
        "totalMainMemoryMb" -> totalMainMemoryMb,
        "taskOverheadMemMb" -> taskOverheadMemMb
      ))

    def requestContainerForPartition(lp: Int) = {
      val taskHeapMemMb = taskOverheadMemMb * 6 / 8
      val taskDirectMemMb = totalMainMemoryMb / numPartitions + (taskOverheadMemMb - taskHeapMemMb)
      val args: Array[String] = Array(ui.serverUrl.toString, lp.toString, numPartitions.toString) ++ topics
      new YarnContainerRequest(taskClass, args, taskPriority, taskDirectMemMb, taskHeapMemMb, 1)
    }

    if (config.containsKey("debug.partitions")) {
      partitionList = config.getProperty("debug.partitions", "0").split(",").toList.map(_.toInt)
    } else {
      partitionList = (0 to numPartitions - 1).toList
    }
    requestContainerGroup(partitionList.map(requestContainerForPartition).toArray)

  }

  override protected def onContainerLaunched(id: String, spec: YarnContainerContext): Unit = {
    val partition = spec.args(2).toInt // FIXME this is a wrong association
    ui.updateMetric(partition,  Metrics.CONTAINER_ID, classOf[metrics.Status], id, spec.getNodeAddress)
    ui.updateMetric(partition, Metrics.CONTAINER_LOGS, classOf[metrics.Status],
      s"<a href='${spec.getLogsUrl("stderr")}'>stderr</a>&nbsp;<a href='${spec.getLogsUrl("stdout")}'>stdout</a>")
    ui.updateMetric(partition, Metrics.CONTAINER_MEMORY, classOf[metrics.Counter], spec.capability.getMemory)
    ui.updateMetric(partition, Metrics.CONTAINER_CORES, classOf[metrics.Counter], spec.capability.getVirtualCores)
  }

  /**
   * Instead of reporting progress as ratio of completed tasks which is more suitable for finite jobs,
   * in streaming progress can be seen as an inverse of lag
   * @return average percentage representation of how the underlying fetchers are caught up with available data.
   */
  final protected override def getProgress: Float = {
    math.min(1f, math.max(0f, ui.getLatestProgress))
  }

  override protected def onContainerCompleted(id: String, spec: YarnContainerContext, exitStatus: Int): Unit = {
    val partition = spec.args(2).toInt // FIXME this is a wrong association
    ui.updateMetric(partition, Metrics.LAST_EXIT_STATUS, classOf[metrics.Status], exitStatus, id)
  }

  /**
   * Yarn Master Completion hook
   */
  override protected def onCompletion: Unit = {
    if (ui != null) ui.stopServer
  }


}


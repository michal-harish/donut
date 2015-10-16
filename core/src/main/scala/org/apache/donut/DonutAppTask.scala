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

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * Created by mharis on 14/09/15.
 *
 * The task may either be run in an executor or yarn container
 * @param config - initial configuration for the entire application
 * @param logicalPartition - the index of logical partition for this task
 * @param totalLogicalPartitions - number of logical partitions determined by the DonutApp
 * @param topics - list of topics to consume
 *
 */

abstract class DonutAppTask(config: Properties, val logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends Runnable {

  private val log = LoggerFactory.getLogger(classOf[DonutAppTask])
  protected[donut] val kafkaUtils = KafkaUtils(config)

  private val topicPartitions: Map[String, Int] = kafkaUtils.getPartitionMap(topics)

  protected val partitionsToConsume: Map[String, Seq[Int]] = topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % totalLogicalPartitions == logicalPartition))
  }
  }

  protected val numFetchers = partitionsToConsume.map(_._2.size).sum

  private val fetcherMonitor = new AtomicReference[Throwable](null)

  private var bootSequenceCompleted = false

  private[donut] val bootSequence = new ConcurrentHashMap[String, Boolean]()

  private[donut] def executeCommand(cmd: String) = {}

  private var masterTrackingUrl: URL = null

  def registerWithMasterTracking(url: URL) = {
    this.masterTrackingUrl = url
    postTrackingInfo(Map("type" -> this.getClass.getSimpleName))
  }

  protected def awaitingTermination

  protected def onShutdown

  protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher

  private[donut] def checkBootSequenceCompleted: Boolean = {
    if (bootSequenceCompleted) {
      true
    } else bootSequence.synchronized {
      if (bootSequenceCompleted) {
        true
      } else if (bootSequence.values().asScala.forall(booted => booted)) {
        log.info("Boot sequence completed!")
        bootSequenceCompleted = true
        bootSequence.notifyAll
        true
      } else {
        false
      }
    }
  }

  final protected[donut] def propagateException(e: Throwable): Unit = {
    fetcherMonitor.synchronized {
      fetcherMonitor.set(e)
      fetcherMonitor.notify
    }
  }

  final override def run: Unit = {
    log.info(s"Starting Task for logical partition ${logicalPartition}/${totalLogicalPartitions} of topics ${topics}")
    val fetchers = partitionsToConsume.flatMap {
      case (topic, partitions) => partitions.map(partition => {
        log.info(s"Initializing fetcher for physical partition ${topic}/$partition in group ${config.getProperty("group.id")}")
        createFetcher(topic, partition, config.getProperty("group.id"))
      })
    }
    bootSequenceCompleted = bootSequence.size == 0 || checkBootSequenceCompleted
    if (bootSequenceCompleted) {
      log.info(s"No boot sequence required, launching ${fetchers.size} fetchers.")
    }
    else {
      log.info("Initializing boot sequence: " + bootSequence.asScala.filter(!_._2).map(_._1).mkString(","))
    }
    val executor = Executors.newFixedThreadPool(fetchers.size)
    fetchers.foreach(fetcher => executor.submit(fetcher))
    executor.shutdown
    try {
      while (!executor.isTerminated) {
        fetcherMonitor.synchronized {
          fetcherMonitor.wait(TimeUnit.SECONDS.toMillis(30))
        }
        if (fetcherMonitor.get != null) {
          throw new Exception(s"Error in task for logical partition ${logicalPartition}", fetcherMonitor.get)
        }
        awaitingTermination
      }
    } catch {
      case e: Throwable => {
        log.error("Task terminated with error", e)
        executor.shutdown
        throw e
      }
    } finally {
      executor.awaitTermination(1, TimeUnit.SECONDS)
      onShutdown
    }
  }


  private def postTrackingInfo(map: Map[String,String]) = {
    val params = map + ("p" -> logicalPartition.toString)
    val uri = "?" + params.map { case (k, v) => s"${k}=${URLEncoder.encode(v, "UTF-8")}" }.mkString("&")
    val url = new URL(masterTrackingUrl, uri)
    log.info(s"POST ${url.toString}")
    val c = url.openConnection.asInstanceOf[HttpURLConnection]
    try {
      c.setRequestMethod("POST")
      c.setRequestProperty("User-Agent", "DonutApp")
      c.setRequestProperty("Accept-Language", "en-US,en;q=0.5")
      if (c.getResponseCode != 202) {
        log.warn(s"POST not accepted by the master tracker at ${masterTrackingUrl}, request = ${params}" +
          s", response = ${c.getResponseCode}: ")
      }
    } finally {
      c.disconnect
    }
  }

}
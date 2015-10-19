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

import java.net.URL
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, TimeUnit}

import org.apache.donut.metrics.{Metrics, Progress}
import org.apache.donut.ui.{UI, WebUI}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * Created by mharis on 14/09/15.
 *
 * The task may either be run in an executor or yarn container
 */
abstract class DonutAppTask(val config: Properties, args: Array[String]) extends Runnable {

  val trackingUrl: URL = new URL(args(0))

  val partition: Int = Integer.valueOf(args(1))

  def tryOrTrack[T](masterUrl: URL, arg: => T): T = try {
    arg
  } catch {
    case e: Throwable => {
      new WebUI(masterUrl).updateError(partition, e)
      throw e
    }
  }

  val numPartitions: Int = tryOrTrack(new URL(args(0)), Integer.valueOf(args(2)))

  val topics: Seq[String] = tryOrTrack(new URL(args(0)), (3 to args.length - 1).map(args(_)))

  private val log = tryOrTrack(trackingUrl, LoggerFactory.getLogger(classOf[DonutAppTask]))

  protected[donut] val kafkaUtils: KafkaUtils = tryOrTrack(trackingUrl, new KafkaUtils(config))

  private val topicPartitions: Map[String, Int] = tryOrTrack(trackingUrl, kafkaUtils.getPartitionMap(topics))

  protected val partitionsToConsume: Map[String, Seq[Int]] = tryOrTrack(trackingUrl,
    topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % numPartitions == partition))
  }})

  protected val numFetchers = tryOrTrack(trackingUrl, partitionsToConsume.map(_._2.size).sum)

  private val fetcherMonitor = tryOrTrack(trackingUrl, new AtomicReference[Throwable](null))

  protected[donut] val ui: UI = tryOrTrack(trackingUrl, new WebUI(trackingUrl))

  private[donut] val bootSequence = tryOrTrack(trackingUrl, new ConcurrentHashMap[String, Boolean]())

  @volatile private var bootSequenceCompleted = false

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
    var executor: ExecutorService = null
    try {
      log.info(s"Starting Task for logical partition ${partition}/${numPartitions} of topics ${topics}")
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
      executor = Executors.newFixedThreadPool(fetchers.size)
      fetchers.foreach(fetcher => executor.submit(fetcher))
      executor.shutdown
      while (!executor.isTerminated) {
        val progressHint = if (!bootSequenceCompleted) "state bootstrap progress.." else "delta input progress.."
        val progress = fetchers.filter(_.isInstanceOf[FetcherBootstrap] ^ bootSequenceCompleted).map(f => {
          val (start, end) = f.getProgressRange
          ((f.getNextFetchOffset.toDouble - start) / (end - start)).toFloat
        }).toSeq
        ui.updateMetric(partition, Metrics.INPUT_PROGRESS, classOf[Progress], progress.sum / progress.size, progressHint)

        fetcherMonitor.synchronized {
          fetcherMonitor.wait(TimeUnit.SECONDS.toMillis(30))
        }
        if (fetcherMonitor.get != null) {
          throw fetcherMonitor.get
        }
        awaitingTermination
      }
    } catch {
      case e: Throwable => {
        log.error("Task terminated with error", e)
        ui.updateError(partition, e)
        try {
          if (executor != null) executor.shutdown
        } finally {
          throw e
        }
      }
    }
  }

}
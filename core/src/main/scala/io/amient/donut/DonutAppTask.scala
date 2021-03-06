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

import java.net.URL
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, TimeUnit}

import io.amient.donut.metrics.{Progress, Metrics}
import io.amient.donut.ui.{WebUI, UI}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * Created by mharis on 14/09/15.
 *
 * The task may either be run in an executor or yarn container
 */
abstract class DonutAppTask(val config: Properties, args: Array[String]) extends Runnable {

  private val trackingUrl: URL = new URL(args(0))

  val partition: Int = Integer.valueOf(args(1))

  def tryOrReport[T](arg: => T): T = try {
    arg
  } catch {
    case e: Throwable => {
      new WebUI(trackingUrl).updateError(partition, e)
      throw e
    }
  }

  val numPartitions: Int = tryOrReport(Integer.valueOf(args(2)))

  val topics: Seq[String] = tryOrReport((3 to args.length - 1).map(args(_)))

  private val log = tryOrReport(LoggerFactory.getLogger(classOf[DonutAppTask]))

  protected[donut] val kafkaUtils: KafkaUtils = tryOrReport(new KafkaUtils(config))

  private val topicPartitions: Map[String, Int] = tryOrReport(kafkaUtils.getPartitionMap(topics))

  protected val partitionsToConsume: Map[String, Seq[Int]] = tryOrReport(
    topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % numPartitions == partition))
  }})

  protected val numFetchers = tryOrReport(partitionsToConsume.map(_._2.size).sum)

  private val fetcherMonitor = tryOrReport(new AtomicReference[Throwable](null))

  protected[donut] val ui: UI = tryOrReport(new WebUI(trackingUrl))

  private[donut] val bootSequence = tryOrReport(new ConcurrentHashMap[String, Boolean]())

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
      var prevProgress: Float = 0f
      while (!executor.isTerminated) {

        //update progress indicator
        val progress = fetchers.filter(_.isInstanceOf[FetcherBootstrap] ^ bootSequenceCompleted).map(f => {
          val (start, end) = f.getProgressRange
          ((f.getNextFetchOffset.toDouble - start) / (end - start)).toFloat
        }).toSeq
        if (progress.size > 0) {
          val newProgress = (progress.sum / progress.size)
          if (newProgress != prevProgress) {
            val hint = if (!bootSequenceCompleted) "state bootstrap progress.." else "delta input progress.."
            ui.updateMetric(partition, Metrics.INPUT_PROGRESS, classOf[Progress], newProgress, hint)
            prevProgress = newProgress
          }
        }

        //hand the control thread to the application for a moment
        awaitingTermination

        //sleep a bit and again
        fetcherMonitor.synchronized {
          fetcherMonitor.wait(TimeUnit.SECONDS.toMillis(30))
        }
        if (fetcherMonitor.get != null) {
          throw fetcherMonitor.get
        }
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
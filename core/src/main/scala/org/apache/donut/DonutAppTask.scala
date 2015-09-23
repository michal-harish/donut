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
  val kafkaUtils = KafkaUtils(config)

  private val topicPartitions: Map[String, Int] = kafkaUtils.getPartitionMap(topics)

  protected val partitionsToConsume: Map[String, Seq[Int]] = topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % totalLogicalPartitions == logicalPartition))
  }
  }

  private val fetcherMonitor = new AtomicReference[(Fetcher,Throwable)](null)

  private var bootSequenceCompleted = false

  private[donut] val bootSequence = new ConcurrentHashMap[String, Boolean]()

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

  final private[donut] def handleFetcherError(fetcher: Fetcher, e: Throwable): Unit = {
    fetcherMonitor.synchronized{
      fetcherMonitor.set((fetcher, e))
      fetcherMonitor.notify
    }
  }

  final override def run: Unit = {
    log.info(s"Starting Task for logical partition ${logicalPartition}/${totalLogicalPartitions}")
    val fetchers = partitionsToConsume.flatMap {
      case (topic, partitions) => partitions.map(partition => {
        createFetcher(topic, partition, config.getProperty("kafka.group.id"))
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
        if (Thread.interrupted) throw new InterruptedException
        fetcherMonitor.synchronized {
          fetcherMonitor.wait(TimeUnit.SECONDS.toMillis(30))
          if (fetcherMonitor.get != null) {
            executor.shutdownNow
            throw new Exception(s"Error in fetcher ${fetcherMonitor.get._1}", fetcherMonitor.get._2)
          }
        }
        awaitingTermination
      }
    } catch {
      case e: Throwable => log.warn("Task terminated", e)
    } finally {
      onShutdown
    }
  }


}
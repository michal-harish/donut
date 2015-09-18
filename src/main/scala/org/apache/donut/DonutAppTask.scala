package org.apache.donut

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration


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

abstract class DonutAppTask(config: Configuration, val logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends Runnable {

  private val log = LoggerFactory.getLogger(classOf[DonutAppTask])
  val kafkaUtils = KafkaUtils(config)

  private val topicPartitions: Map[String, Int] = kafkaUtils.getPartitionMap(topics)

  protected val partitionsToConsume: Map[String, Seq[Int]] = topicPartitions.map { case (topic, numPhysicalPartitions) => {
    (topic, (0 to numPhysicalPartitions - 1).filter(_ % totalLogicalPartitions == logicalPartition))
  }
  }

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


  final override def run: Unit = {
    log.info(s"Starting Donut Task for logical partition ${logicalPartition}/${totalLogicalPartitions}")
    val fetchers = partitionsToConsume.flatMap {
      case (topic, partitions) => partitions.map(partition => {
        createFetcher(topic, partition, config.get("kafka.group.id"))
      })
    }
    bootSequenceCompleted = bootSequence.size == 0 || checkBootSequenceCompleted
    if (bootSequenceCompleted) {
      log.info("No boot sequence required, launching.")
    }
    else {
      log.info("Initializing boot sequence: " + bootSequence.asScala.filter(!_._2).map(_._1).mkString(","))
    }
    val executor = Executors.newFixedThreadPool(fetchers.size)
    log.info(s"NUM FETCHERS = ${fetchers.size}")
    fetchers.foreach(fetcher => executor.submit(fetcher))
    //TODO handle fetcher failures and close (it's actually happening but don't know why)
    executor.shutdown
    try {
      while (!Thread.interrupted()) {
        if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
          return
        } else {
          awaitingTermination
        }
      }
    } finally {
      onShutdown
    }
  }


}
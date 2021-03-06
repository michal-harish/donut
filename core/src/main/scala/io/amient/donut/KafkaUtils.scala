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

import java.io.IOException
import java.util.Properties

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.producer.{KeyedMessage, Partitioner, Producer, ProducerConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Created by mharis on 14/09/15.
 */
class KafkaUtils(val config: Properties) {

  type MESSAGE = KeyedMessage[Array[Byte], Array[Byte]]
  private val log = LoggerFactory.getLogger(classOf[DonutApp[_]])

  val kafkaBrokers = config.getProperty("kafka.brokers")
  val soTimeout: Int = 100000
  val bufferSize: Int = 64 * 1024

  def getPartitionMap(topics: Seq[String]): Map[String, Int] = getMetadata((seed: SimpleConsumer) => {
    val req = new TopicMetadataRequest(topics, 0)
    val resp = seed.send(req)
    resp.topicsMetadata.map(tm => (tm.topic, tm.partitionsMetadata.size)).toMap
  })

  def getNumLogicalPartitions(topics: Seq[String]): Int = {
    val cogroup = config.getProperty("cogroup", "false").toBoolean
    val topicParts = getPartitionMap(topics)
    val maxParts = topicParts.map(_._2).max
    val maxTasks = math.min(maxParts, config.getProperty("max.tasks", maxParts.toString).toInt)
    val result = if (cogroup) {
      (1 to maxTasks).reverse.filter(potentialHCF => {
        topicParts.forall { case (topic, numPartitions) => numPartitions % potentialHCF == 0 }
      }).max
    } else {
      maxTasks
    }
    log.info(s"numLogicalPartitions (cogroup = ${cogroup}})(${topicParts.mkString(",")}}) = " + result)
    return result
  }

  def getPartitionMeta(topic: String, partition: Int): PartitionMetadata = {
    getMetadata((seed: SimpleConsumer) => {
      val req = new TopicMetadataRequest(Seq(topic), 0)
      seed.send(req).topicsMetadata.head.partitionsMetadata.filter(_.partitionId == partition).head
    })
  }

  def findLeader(topic: String, partition: Int): Broker = {
    while (true) {
      val partitionMeta = getPartitionMeta(topic, partition)
      if (partitionMeta == null) {
        throw new IllegalArgumentException(s"Empty partition metadata for ${topic}/${partition}")
      } else if (partitionMeta.leader.isEmpty) {
        log.warn(s"No partition leader ${topic}/${partition}, retrying in 10s ...")
        Thread.sleep(10000)
      } else {
        return partitionMeta.leader.get
      }
    }
    throw new IllegalStateException
  }

  def getOffsetRange(consumer: SimpleConsumer, topicAndPartition: TopicAndPartition, earliestOrLatest: Long): Long = {
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(earliestOrLatest, 1))
    val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      throw new IOException("Error fetching data Offset Data the Broker: " + response.describe(true))
    }
    val offsets = response.offsetsGroupedByTopic(topicAndPartition.topic).head._2.offsets
    return offsets(0)
  }

  /**
   * getGroupOffset - compute an average progress between 0.0 and 1.0 for all partitions in the
   * listed topics in a given consumer group. This is quite an expensive operation which
   * queries kafka brokers for metadata so should not be called too frequently.
   *
   * @return next offset to be consumed in the given partition
   */
  def getGroupOffset(groupId: String, topicAndPartition: TopicAndPartition): Long = {
    getCoordinator(groupId) match {
      case None => -1L
      case Some(coordinator) => {
        val consumer = new SimpleConsumer(coordinator.host, coordinator.port, 100000, 64 * 1024, "consumerOffsetFetcher")
        try {
          val req = new OffsetFetchRequest(groupId, Seq(topicAndPartition))
          val res = consumer.fetchOffsets(req)
          //TODO handle errors, but fetchOffsets response doesn't have hasError method
          val consumed = res.requestInfo(topicAndPartition).offset
          consumed
        } finally {
          consumer.close
        }
      }
    }
  }

  /**
   * calculate a progress value ( 0.0 to 1.0) for given consumer group
   * @param consumerGroupId
   * @param topics
   * @return (min, avg, max) progress aggregated from all partitions in the given list of topics
   */
  def getGroupProgress(consumerGroupId: String, topics: List[String]): (Float, Float, Float) = {
    val progress = getPartitionMap(topics).flatMap { case (topic, numPartitions) => {
      (0 to numPartitions - 1).map(p => {
        val consumer = new PartitionConsumer(topic, p, consumerGroupId)
        try {
          val (earliest, consumed, latest) = (consumer.getEarliestOffset, consumer.getOffset, consumer.getLatestOffset)
          math.min(1f, math.max(0f, (consumed - earliest).toFloat / (latest - earliest).toFloat))
        } finally {
          consumer.close
        }
      })
    }
    }
    if (progress.size == 0) (0, 0, 0) else (progress.min, progress.sum / progress.size, progress.max)
  }

  protected def commitGroupOffset(groupId: String, topicAndPartition: TopicAndPartition, offset: Long, failOnError: Boolean): Unit = {
    var numErrors = 0
    while (true) {
      try {
        getCoordinator(groupId) match {
          case None => throw new IllegalStateException
          case Some(coordinator) => {
            val consumer = new SimpleConsumer(coordinator.host, coordinator.port, 100000, 64 * 1024, "consumerOffsetCommitter")
            try {
              val req = new OffsetCommitRequest(groupId, Map(topicAndPartition -> OffsetAndMetadata(offset)))
              val res = consumer.commitOffsets(req)
              if (res.hasError) {
                throw new IOException(res.describe(true))
              }
              return
            } finally {
              consumer.close
            }
          }
        }
      } catch {
        case e: IOException => {
          numErrors += 1
          if (failOnError && numErrors > 3) throw e
          log.warn(s"Error committing offset for consumer group `${groupId}`", e)
          if (failOnError) Thread.sleep(10000) else return
        }
      }
    }
  }

  private def getCoordinator(groupId: String): Option[Broker] = getMetadata((seed) => {
    val req = new ConsumerMetadataRequest(groupId)
    seed.send(req).coordinatorOpt
  })

  private def getMetadata[R](f: (SimpleConsumer) => R): R = {
    val it = kafkaBrokers.split(",").iterator
    while (it.hasNext) {
      val Array(host, port) = it.next.split(":")
      try {
        val consumer = new SimpleConsumer(host, port.toInt, 100000, 64 * 1024, "leaderLookup")
        try {
          return f(consumer)
        } finally {
          consumer.close
        }
      } catch {
        case e: Throwable => log.error(s"Problem occurred while communicating with kafka admin api ${host}:${port} ", e)
      }
    }
    throw new IOException("Could not establish connection with any of the seed brokers")
  }

  class PartitionConsumer(topic: String, partition: Int, val leader: Broker, soTimeout: Int, bufferSize: Int, val groupId: String)
    extends SimpleConsumer(leader.host, leader.port, soTimeout, bufferSize, s"${groupId}_${topic}_${partition}") {

    def this(topic: String, partition: Int, groupId: String) {
      this(topic, partition, findLeader(topic, partition), 100000, 64 * 1024, groupId)
    }

    val topicAndPartition = new TopicAndPartition(topic, partition)

    def fetch(readOffset: Long, fetchSize: Int): FetchResponse = {
      fetch(new FetchRequestBuilder()
        .clientId(this.clientId)
        .addFetch(topic, partition, readOffset, fetchSize)
        .build())
    }

    def getEarliestOffset: Long = getOffsetRange(this, topicAndPartition, kafka.api.OffsetRequest.EarliestTime)

    def getLatestOffset: Long = getOffsetRange(this, topicAndPartition, kafka.api.OffsetRequest.LatestTime)

    def getOffset: Long = getGroupOffset(groupId, topicAndPartition)

    def commitOffset(offset: Long, failOnError: Boolean): Unit = commitGroupOffset(groupId, topicAndPartition, offset, failOnError)

  }

  /**
   * Kafka producer helper - we need to treat the compaction-deletion and sync-async asepcts as separate concerns
   * because: 1) compacted topic cannot use compression 2) async producers may call the encoder at later stage
   * so cannot use zero-copy buffers
   */

  def createProducer[P <: Partitioner](configNameSpace: String)(implicit p: Manifest[P]) = {
    new Producer[Array[Byte], Array[Byte]](new ProducerConfig(new java.util.Properties {
      config.keySet.asScala.map(_.toString).filter(_.startsWith(s"${configNameSpace}.")).foreach(
        param => put(param.substring(configNameSpace.length + 1), config.get(param))
      )
      put("partitioner.class", p.runtimeClass.getName)
    }))
  }

}

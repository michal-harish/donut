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

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Properties

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{Consumer, ConsumerConfig, SimpleConsumer}
import kafka.producer.{KeyedMessage, Partitioner, Producer, ProducerConfig}
import kafka.serializer.DefaultEncoder
import org.slf4j.LoggerFactory

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
      try {
        val partitionMeta = getPartitionMeta(topic, partition)
        if (partitionMeta == null) {
          throw new IllegalArgumentException(s"Empty partition metadata for ${topic}/${partition}")
        } else if (partitionMeta.leader.isEmpty) {
          throw new IllegalStateException
        } else {
          return partitionMeta.leader.get
        }
      } catch {
        case e: IllegalStateException => {
          log.warn(s"No partition leader ${topic}/${partition}, retrying in 10s ...")
          Thread.sleep(10000)
        }
      }
    }
    throw new IllegalStateException
  }

  def getOffsetRange(consumer: SimpleConsumer, topicAndPartition: TopicAndPartition, earliestOrLatest: Long): Long = {
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(earliestOrLatest, 1))
    val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      throw new Exception("Error fetching data Offset Data the Broker: " + response.describe(true))
    }
    val offsets = response.offsetsGroupedByTopic(topicAndPartition.topic).head._2.offsets
    return offsets(0)
  }

  /**
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
    throw new Exception("Could not establish connection with any of the seed brokers")
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
   * Kafka producer helpers - we need to treat the compaction-deletion and sync-async asepcts as separate concerns
   * because: 1) compacted topic cannot use compression 2) async producers may call the encoder at later stage
   * so cannot use zero-copy buffers
   */

  def snappySyncProducer[P <: Partitioner](numAcks: Int)(implicit p: Manifest[P]) = {
    new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
      put("metadata.broker.list", config.get("kafka.brokers"))
      put("request.required.acks", numAcks.toString)
      put("serializer.class", classOf[KafkaByteBufferEncoder].getName)
      put("partitioner.class", p.runtimeClass.getName)
      put("compression.codec", "2") //SNAPPY
      put("producer.type", "sync")
    }))
  }

  def snappyAsyncProducer[P <: Partitioner](numAcks: Int, batchSize: Int = 200, queueSize: Int = 5000)(implicit p: Manifest[P]) = {
    new Producer[Array[Byte], Array[Byte]](new ProducerConfig(new java.util.Properties {
      put("metadata.broker.list", config.get("kafka.brokers"))
      put("request.required.acks", numAcks.toString)
      put("serializer.class", classOf[DefaultEncoder].getName)
      put("partitioner.class", p.runtimeClass.getName)
      put("compression.codec", "2") //SNAPPY
      put("producer.type", "async")
    }))
  }

  def compactSyncProducer[P <: Partitioner](numAcks: Int)(implicit p: Manifest[P]) = {
    new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
      put("metadata.broker.list", config.get("kafka.brokers"))
      put("request.required.acks", numAcks.toString)
      put("serializer.class", classOf[KafkaByteBufferEncoder].getName)
      put("partitioner.class", p.runtimeClass.getName)
      put("compression.codec", "0") //NONE - Kafka Log Compaction doesn't work for compressed topics
      put("producer.type", "sync")
    }))
  }

  def compactAsyncProducer[P <: Partitioner](numAcks: Int, batchSize: Int = 200, queueSize: Int = 10000)(implicit p: Manifest[P]) = {
    new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
      put("metadata.broker.list", config.get("kafka.brokers"))
      put("request.required.acks", numAcks.toString)
      put("serializer.class", classOf[KafkaByteBufferEncoder].getName)
      put("partitioner.class", p.runtimeClass.getName)
      put("compression.codec", "0") //NONE - Kafka Log Compaction doesn't work for compressed topics
      put("producer.type", "async")
      put("batch.num.messages", batchSize.toString)
      put("queue.buffering.max.messages", queueSize.toString)
    }))
  }

  /**
   * Debuggin tools
   */

  def createDebugConsumer(topic: String, processor: (ByteBuffer, ByteBuffer) => Unit) = {
    val consumer = Consumer.create(new ConsumerConfig(new Properties() {
      put("group.id", "DonutDebugger")
      put("zookeeper.connect", config.get("zookeeper.connect"))
    }))
    val stream = consumer.createMessageStreams(Map(topic -> 1))(topic).head
    for (msg <- stream) {
      processor(ByteBuffer.wrap(msg.key), ByteBuffer.wrap(msg.message))
    }
  }

}

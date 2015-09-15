package org.apache.donut

import java.io.IOException

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{OffsetAndMetadata, ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 14/09/15.
 */
case class KafkaUtils(val conf: Configuration) {


  private val log = LoggerFactory.getLogger(classOf[DonutApp[_]])

  val kafkaBrokers = conf.get("kafka.brokers")
  val soTimeout: Int = 100000
  val bufferSize: Int = 64 * 1024

  def getPartitionMap(topics: Seq[String]): Map[String, Int] = getMetadata((seed: SimpleConsumer) => {
    val req = new TopicMetadataRequest(topics, 0)
    val resp = seed.send(req)
    resp.topicsMetadata.map(tm => (tm.topic, tm.partitionsMetadata.size)).toMap
  })

  def getNumLogicalPartitions(topics: Seq[String], cogroup: Boolean): Int = {
    val topicParts = getPartitionMap(topics)
    val maxParts = topicParts.map(_._2).max
    val result = if (cogroup) {
      (1 to maxParts).reverse.filter(potentialHCF => {
        topicParts.forall { case (topic, numPartitions) => numPartitions % potentialHCF == 0 }
      }).max
    } else {
      maxParts
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
    val partitionMeta = getPartitionMeta(topic, partition)
    if (partitionMeta == null) {
      throw new IllegalStateException(s"Empty partition metadata for ${topic}/${partition}")
    } else if (partitionMeta.leader.isEmpty) {
      throw new IllegalStateException(s"No partition leader ${topic}/${partition}")
    }
    partitionMeta.leader.get
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

  protected def commitGroupOffset(groupId: String, topicAndPartition: TopicAndPartition, offset: Long) = {
    getCoordinator(groupId) match {
      case None => throw new IllegalStateException
      case Some(coordinator) => {
        val consumer = new SimpleConsumer(coordinator.host, coordinator.port, 100000, 64 * 1024, "consumerOffsetCommitter")
        try {
          val req = new OffsetCommitRequest(groupId, Map(topicAndPartition -> OffsetAndMetadata(offset)))
          if (consumer.commitOffsets(req).hasError) throw new Exception(s"Error committing offset for conumser group `${groupId}`")
        } finally {
          consumer.close
        }
      }
    }
  }

  /**
   * @return (erliest available, next to be consumed, latest available)
   */
  protected def getGroupOffset(groupId: String, topicAndPartition: TopicAndPartition): Long = {
    getCoordinator(groupId) match {
      case None => -1L
      case Some(coordinator) => {
        val consumer = new SimpleConsumer(coordinator.host, coordinator.port, 100000, 64 * 1024, "consumerOffsetFetcher")
        try {
          val req = new OffsetFetchRequest(groupId, Seq(topicAndPartition))
          //FIXME handle errors, but fetchOffsets response doesn't have hasError method
          val consumed = consumer.fetchOffsets(req).requestInfo(topicAndPartition).offset
          consumed
        } finally {
          consumer.close
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
      this(topic, partition, findLeader(topic, partition), 100000, 64 * 1024, s"${groupId}_${topic}_${partition}")
    }

    val topicAndPartition = new TopicAndPartition(topic, partition)

    def fetch(readOffset: Long, fetchSize: Int): FetchResponse = {
      fetch(new FetchRequestBuilder().clientId(this.clientId).addFetch(topic, partition, readOffset, fetchSize).build())
    }

    def getEarliestOffset: Long = getOffsetRange(this, topicAndPartition, kafka.api.OffsetRequest.EarliestTime)

    def getLatestOffset: Long = getOffsetRange(this, topicAndPartition, kafka.api.OffsetRequest.LatestTime)

    def getOffset: Long = getGroupOffset(groupId, topicAndPartition)

    def commitOffset(offset: Long) : Unit = commitGroupOffset(groupId, topicAndPartition, offset)

  }

}

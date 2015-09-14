package org.apache.donut

import java.io.IOException

import kafka.api._
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

/**
 * Created by mharis on 14/09/15.
 */
case class KafkaUtils(val conf: Configuration) {

  private val log = LoggerFactory.getLogger(classOf[DonutApp[_]])

  val kafkaBrokers = conf.get("donut.kafka.brokers")
  val port = conf.getInt("donut.kafka.port", 9092)
  val soTimeout: Int = 100000
  val bufferSize: Int = 64 * 1024

  def getPartitionMap(topics: Seq[String]): Map[String, Int] = getMetadata((seed: SimpleConsumer) => {
    val req = new TopicMetadataRequest(topics, 0)
    val resp = seed.send(req)
    resp.topicsMetadata.map(tm => (tm.topic, tm.partitionsMetadata.size)).toMap
  })

  def getNumLogicalPartitions(topics: Seq[String], cogroup: Boolean): Int =  {
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

  private def getMetadata[R](f: (SimpleConsumer) => R): R = {
    val it = kafkaBrokers.split(",").iterator
    while (it.hasNext) {
      val host = it.next
      try {
        val consumer: SimpleConsumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "leaderLookup")
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

  def findLeader(topic: String, partition: Int) = {
    val partitionMeta = getPartitionMeta(topic, partition)
    if (partitionMeta == null) {
      throw new IllegalStateException(s"Empty partition metadata for ${topic}/${partition}")
    } else if (partitionMeta.leader.isEmpty) {
      throw new IllegalStateException(s"No partition leader ${topic}/${partition}")
    }
    partitionMeta.leader.get.host
  }

  def getEarliestOffset(consumer: SimpleConsumer , topic: String , partition: Int, clientName: String ): Long = {
    val topicAndPartition = new TopicAndPartition(topic, partition)
    val requestInfo = Map(topicAndPartition -> new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime, 1))
    val request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion,0 ,clientName)
    val response = consumer.getOffsetsBefore(request)
    if (response.hasError) {
      throw new IOException("Error fetching data Offset Data the Broker")
    }
    val offsets = response.offsetsGroupedByTopic(topic).head._2.offsets
    return offsets(0)
  }

}

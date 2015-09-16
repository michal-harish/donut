package net.imagini.dxp.graphbsp

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import net.imagini.dxp.common._
import org.apache.donut._
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamProcessUnit(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val bspIn = new AtomicLong(0)
  val bspEvicted = new AtomicLong(0)
  val bspMiss = new AtomicLong(0)
  val bspUpdated = new AtomicLong(0)
  val stateIn = new AtomicLong(0)

  private val localState = new LocalStorage[ByteBuffer](5000000)

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {

        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          localState.put(messageAndOffset.message.key, messageAndOffset.message.payload)
          stateIn.incrementAndGet
        }
      }

      case "graphstream" => new FetcherOnce(this, topic, partition, groupId) {
        val MAX_ITER = 5
        private val MAX_EDGES = 69

        override def handleMessage(envelope: MessageAndOffset): Unit = {
          bspIn.incrementAndGet
          val key = envelope.message.key
          localState.get(key) match {
            case None => {
              bspMiss.incrementAndGet
              setState(key, envelope.message.payload)
            }
            case Some(null) => bspEvicted.incrementAndGet
            case Some(state) => {
              bspUpdated.incrementAndGet
              val (iteration, inputEdges) = BSPMessage.decodePayload(envelope.message.payload)
              val existingEdges = BSPMessage.decodePayload(state)._2
              val additionalEdges = inputEdges.filter(n => !existingEdges.contains(n._1))
              val newEdges = existingEdges ++ additionalEdges
              val newState = if (newEdges.size > MAX_EDGES) null else BSPMessage.encodePayload((iteration, newEdges))
              setState(key, newState)
              graphstreamProducer.send(
                new KeyedMessage("graphstate", key, envelope.message.payload))
              if (iteration < MAX_ITER) {

              }
            }
          }
        }
      }

    }
  }

  private val graphstreamProducer = new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", config.get("kafka.brokers"))
    put("request.required.acks", "0")
    put("producer.type", "async")
    put("serializer.class", classOf[ByteBufferEncoder].getName)
    put("partitioner.class", classOf[KafkaVidPartitioner].getName)
    put("batch.num.messages", "500")
    put("compression.codec", "2") //SNAPPY
  }))

  //TODO alter topic `graphstate` to use log compaction
  private val stateProducer = new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", config.get("kafka.brokers"))
    put("request.required.acks", "0")
    put("producer.type", "async")
    put("serializer.class", classOf[ByteBufferEncoder].getName)
    put("partitioner.class", classOf[KafkaVidPartitioner].getName)
    put("batch.num.messages", "200")
    put("compression.codec", "0") //NONE - Compaction doesn't work for compressed topics
  }))

  private def setState(key: ByteBuffer, payload: ByteBuffer) {
    localState.put(key, payload)
    graphstreamProducer.send(new KeyedMessage("graphstate", key, payload))
  }

  override def awaitingTermination {
    println(
      s"=> graphstream(${bspIn.get} - evicted ${bspEvicted.get} + missed ${bspMiss.get} + hit ${bspUpdated.get}) " +
        s"=> graphstate(${stateIn.get}) " +
        s"=> state.size = " + localState.size
    )
  }

  override def onShutdown: Unit = {
    graphstreamProducer.close
  }

}


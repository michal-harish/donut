package net.imagini.dxp.graphbsp

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import net.imagini.dxp.common._
import org.apache.donut._
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamProcessUnit(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val zkHosts = config.get("zookeeper.connect")
  val brokers = config.get("kafka.brokers")

  val graphstreamIn = new AtomicLong(0)
  val graphstreamEvicted = new AtomicLong(0)
  val counterMiss = new AtomicLong(0)
  val counterUpdated = new AtomicLong(0)
  val stateIn = new AtomicLong(0)

  val localState = new LocalStorage[Map[Vid, Edge]](500000)
  val MAX_EDGES = 59

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherBootstrap(kafkaUtils, topic, partition, groupId) {
        def asyncProcessMessage(messageAndOffset: MessageAndOffset): Unit = {
          val edges = BSPMessage.decodePayload(messageAndOffset.message.payload)._2
          localState.put(messageAndOffset.message.key, edges)
          stateIn.incrementAndGet
        }
      }

      case "graphstream" => new FetcherOnce(kafkaUtils, topic, partition, groupId) {
        val MAX_ITER = 5

        override def asyncProcessMessage(envelope: MessageAndOffset): Unit = {
          //TODO wait until graphstate fetcher is cought up

          graphstreamIn.incrementAndGet
          val key = envelope.message.key
          var newState: Map[Vid, Edge] = null
          localState.get(key) match {
            case None => {
              counterMiss.incrementAndGet
              newState = BSPMessage.decodePayload(envelope.message.payload)._2
              setState(key, envelope.message.payload, newState)
            }
            case Some(null) => graphstreamEvicted.incrementAndGet
            case Some(existingEdges) => {
              counterUpdated.incrementAndGet
              val (iteration, inputEdges) = BSPMessage.decodePayload(envelope.message.payload)
              val additionalEdges = inputEdges.filter(n => !existingEdges.contains(n._1))
              val newState = existingEdges ++ additionalEdges
              val payload = BSPMessage.encodePayload((iteration, newState))
              setState(key, payload, newState)
              producerWithCompression.send(
                new KeyedMessage("graphstate", envelope.message.key, envelope.message.payload))
              if (iteration < MAX_ITER) {

              }
            }
          }
        }
      }

    }
  }

  private val producerWithCompression = new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", brokers)
    put("request.required.acks", "0")
    put("producer.type", "async")
    put("serializer.class", classOf[ByteBufferEncoder].getName)
    put("partitioner.class", classOf[VidPartitioner].getName)
    put("batch.num.messages", "500")
    put("compression.codec", "2") //SNAPPY
  }))

  //TODO alter topic `graphstate` to use log compaction
  private val producerGraphstate = new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", brokers)
    put("request.required.acks", "0")
    put("producer.type", "async")
    put("serializer.class", classOf[ByteBufferEncoder].getName)
    put("partitioner.class", classOf[VidPartitioner].getName)
    put("batch.num.messages", "200")
    put("compression.codec", "0") //NONE - Compaction doesn't work for compressed topics
  }))

  private def setState(key: ByteBuffer, payload: ByteBuffer, edges: Map[Vid, Edge]) {
    if (edges.size > MAX_EDGES) {
      localState.put(key, null)
      producerWithCompression.send(
        new KeyedMessage("graphstate", key, null))
    } else {
      localState.put(key, edges)
      producerWithCompression.send(
        new KeyedMessage("graphstate", key, payload))
    }
  }

  override def awaitingTermination {
    println(
      s"=> graphstream(${graphstreamIn.get} - evicted ${graphstreamEvicted.get} + missed ${counterMiss.get} + hit ${counterUpdated.get}) " +
        s"=> graphstate(${stateIn.get}) " +
        s"=> state.size = " + localState.size
    )
  }

  override def onShutdown: Unit = {
    producerWithCompression.close
  }

}


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

  val bspIn = new AtomicLong(0)
  val bspEvicted = new AtomicLong(0)
  val bspMiss = new AtomicLong(0)
  val bspUpdated = new AtomicLong(0)
  val stateIn = new AtomicLong(0)

  private val localState = new LocalStorage(5000000)

  private val graphstreamProducer = kafkaUtils.createSnappyProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 1000)

  private val stateProducer = kafkaUtils.createCompactProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 200)

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "graphstate" => new FetcherBootstrap(this, topic, partition, groupId) {

        def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          localState.put(messageAndOffset.message.key, messageAndOffset.message.payload)
          stateIn.incrementAndGet
        }
      }

      case "graphstream" => new FetcherOnce(this, topic, partition, groupId) {
        val MAX_ITER = 3
        private val MAX_EDGES = 9

        override def handleMessage(envelope: MessageAndOffset): Unit = {
          bspIn.incrementAndGet
          val key = envelope.message.key
          localState.get(key) match {
            case None => {
              bspMiss.incrementAndGet
              localState.put(key, envelope.message.payload)
              stateProducer.send(new KeyedMessage("graphstate", key, envelope.message.payload))
            }
            case Some(null) => bspEvicted.incrementAndGet
            case Some(previousState) => {
              bspUpdated.incrementAndGet
              val payload = envelope.message.payload
              val (iteration, inputEdges) = BSPMessage.decodePayload(payload.array, payload.arrayOffset)
              val existingEdges = BSPMessage.decodePayload(previousState)._2
              val additionalEdges = inputEdges.filter(n => !existingEdges.contains(n._1))
              val newEdges = existingEdges ++ additionalEdges
              if (newEdges.size > MAX_EDGES) {
                localState.put(key, null.asInstanceOf[Array[Byte]])
                stateProducer.send(new KeyedMessage("graphstate", key, null))
              } else {
                val newState = BSPMessage.encodePayload((iteration, newEdges))
                localState.put(key, newState)
                if (iteration < MAX_ITER) {
                  val newPayload = ByteBuffer.wrap(BSPMessage.encodePayload(((iteration + 1).toByte, newEdges)))
                  existingEdges.foreach { case (v,props) => {
                    graphstreamProducer.send(
                      new KeyedMessage("graphstream", ByteBuffer.wrap(BSPMessage.encodeKey(v)), newPayload))
                  }}
                  val previousPayload = ByteBuffer.wrap(BSPMessage.encodePayload(((iteration + 1).toByte, existingEdges)))
                  newEdges.foreach { case (v,props) => {
                    graphstreamProducer.send(
                      new KeyedMessage("graphstream", ByteBuffer.wrap(BSPMessage.encodeKey(v)), previousPayload))
                  }}
                }
              }
            }
          }
        }
      }

    }
  }

  override def awaitingTermination {
    println(
      s"=> graphstream(${bspIn.get} - evicted ${bspEvicted.get} + missed ${bspMiss.get} + hit ${bspUpdated.get}) " +
        s"=> graphstate(${stateIn.get}) " +
        s"=> state.size = " + localState.size + ", state.memory = " + localState.minSizeInByte / 1024 / 1024 + " Mb"
    )
  }

  override def onShutdown: Unit = {
    graphstreamProducer.close
  }

}


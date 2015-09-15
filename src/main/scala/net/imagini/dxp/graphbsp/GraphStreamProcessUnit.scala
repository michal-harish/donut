package net.imagini.dxp.graphbsp

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.{Producer, ProducerConfig}
import net.imagini.dxp.common.{VidPartitioner, BSPMessage, Edge, Vid}
import org.apache.donut.{DonutAppTaskRecursive, DonutAppTask, LocalStorage}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

/**
 * Created by mharis on 14/09/15.
 */
class GraphStreamProcessUnit(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTaskRecursive(config, logicalPartition, totalLogicalPartitions, topics) {


  val zkHosts = config.get("zookeeper.connect")
  val brokers = config.get("kafka.brokers")

  val producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", brokers)
    put("request.required.acks", "0")
    put("producer.type", "async")
    put("serializer.class", classOf[kafka.serializer.DefaultEncoder].getName)
    put("partitioner.class", classOf[VidPartitioner].getName)
    put("batch.num.messages", "500")
    put("compression.codec", "2") //SNAPPY
  }))
  //TODO generalise into StreamTransformation

  var lastProcessedOffset = -1L

  val MAX_ITER = 5
  val MAX_EDGES = 59
  val localState = new LocalStorage[mutable.Set[Vid]](500000)
  val counterReceived = new AtomicLong(0)
  val counterEvicted = new AtomicLong(0)
  val counterInitialised = new AtomicLong(0)
  val counterUpdated = new AtomicLong(0)

  override def onShutdown: Unit = {
    producer.close
  }

  override def awaitingTermination {
    println(
        s"=> graphstream(${counterReceived.get}) - evicted(${counterEvicted.get}}) => (${counterInitialised.get} + ${counterUpdated.get})) " +
        s"=> state.size = " + localState.size
    )
  }

  //FIXME under the current api design we are decoding the payload twice, but using kafka encoder is not optimal as we cannot make decision whether it's worth decoding at all
  override def asyncUpdateState(messageAndOffset: MessageAndOffset): Unit = {
    val (iteration, edges) = BSPMessage.decodePayload(messageAndOffset.message.payload)

  }

  override def asyncProcessMessage(messageAndOffset: kafka.message.MessageAndOffset): Unit = {
    val msgOffset = messageAndOffset.offset
    counterReceived.incrementAndGet
    val key = messageAndOffset.message.key
    localState.get(key) match {
      case None => {
        counterInitialised.incrementAndGet
        val (iteration, inputEdges) = BSPMessage.decodePayload(messageAndOffset.message.payload)
        initState(key, inputEdges)
      }
      case Some(null) => counterEvicted.incrementAndGet
      case Some(state) => {
        counterUpdated.incrementAndGet
        val (iteration, inputEdges) = BSPMessage.decodePayload(messageAndOffset.message.payload)
        try {
          if (msgOffset > lastProcessedOffset) {
            process(state, iteration, inputEdges)
            lastProcessedOffset = msgOffset
          }
        } finally {
          updateState(key, state, inputEdges)
        }
      }

    }

    def process(state: mutable.Set[Vid], iteration: Int, inputEdges: Map[Vid, Edge]): Unit = {
      if (iteration < MAX_ITER) {
        val additionalEdges = if (state == null) inputEdges else inputEdges.filter(n => !state.contains(n._1))
        //producer.send(state.map(e => GraphMessage(e, iteration + 1, additionalEdges)).toList)
      }
    }


    def initState(key: ByteBuffer, inputEdges: Map[Vid, Edge]): Unit = {
      if (inputEdges.size > MAX_EDGES) {
        localState.put(key, null)
      } else {
        localState.put(key, mutable.Set(inputEdges.map(_._1).toSeq: _*))
      }
    }

    def updateState(key: ByteBuffer, state: mutable.Set[Vid], inputEdges: Map[Vid, Edge]): Unit = {
      val newEdges = inputEdges.filter(n => !state.contains(n._1))
      if (newEdges.size + state.size > MAX_EDGES) {
        localState.put(key, null)
      } else {
        state ++= newEdges.map(_._1)
      }
    }
  }


}


package net.imagini.dxp.syncstransform

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.{ProducerConfig, Producer, KeyedMessage}
import net.imagini.common.message.VDNAUserImport
import net.imagini.common.messaging.serde.VDNAUniversalDeserializer
import net.imagini.dxp.common._
import org.apache.donut._
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 15/09/15.
 */
class SyncsTransformProcessUnit(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val vdnaMessageDecoder = new VDNAUniversalDeserializer
  val counterReceived = new AtomicLong(0)
  val counterInvalid = new AtomicLong(0)
  val counterFiltered = new AtomicLong(0)
  val counterProduced = new AtomicLong(0)
  val idSpaceSet = Set("a", "r", "d")

  val producer = new Producer[ByteBuffer, ByteBuffer](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", config.get("kafka.brokers"))
    put("request.required.acks", "0")
    put("producer.type", "async")
    put("serializer.class", classOf[KafkaByteBufferEncoder].getName)
    put("partitioner.class", classOf[KafkaRangePartitioner].getName)
    put("batch.num.messages", "500")
    put("compression.codec", "2") //SNAPPY
  }))

  override def onShutdown: Unit = {
    producer.close
  }

  override def awaitingTermination: Unit = {
    println(s"datasync[${counterReceived.get}] => filter[${counterFiltered.get}] => graphstream[${counterProduced.get}] [invalid ${counterInvalid.get}]")
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "datasync" => new FetcherOnce(this, topic, partition, groupId) {
        override def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          val payload = messageAndOffset.message.payload
          //FIXME now that we have ByteBuffers vdna decoder should support offset to deserialize from
          val payloadArray: Array[Byte] = util.Arrays.copyOfRange(payload.array, payload.arrayOffset(), payload.arrayOffset + messageAndOffset.message.payloadSize )
          val vdnaMsg = vdnaMessageDecoder.decodeBytes(payloadArray)
          counterReceived.incrementAndGet

          if (vdnaMsg.isInstanceOf[VDNAUserImport]) {
            val importMsg = vdnaMsg.asInstanceOf[VDNAUserImport]
            if (importMsg.getUserCookied &&
              !importMsg.getUserOptOut &&
              importMsg.getUserUid != null &&
              importMsg.getPartnerUserId != null &&
              idSpaceSet.contains(importMsg.getIdSpace)) {
              counterFiltered.addAndGet(1L)
              transformAndProduce(importMsg)
            }
          }
        }
      }
    }
  }

  def transformAndProduce(importMsg: VDNAUserImport) = {
    try {
      val vdnaId = Vid("vdna", importMsg.getUserUid.toString)
      val partnerId = Vid(importMsg.getIdSpace, importMsg.getPartnerUserId)
      val edge = Edge("AAT", 1.0, importMsg.getTimestamp)
      producer.send(
        new KeyedMessage(
          "graphstream", BSPMessage.encodeKey(vdnaId), BSPMessage.encodePayload((1, Map(partnerId -> edge)))),
        new KeyedMessage(
          "graphstream", BSPMessage.encodeKey(partnerId), BSPMessage.encodePayload((1, Map(vdnaId -> edge))))
      )
      counterProduced.addAndGet(2L)
    } catch {
      case e: IllegalArgumentException => counterInvalid.incrementAndGet
    }
  }

}

package net.imagini.dxp.syncstransform

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.KeyedMessage
import net.imagini.common.message.VDNAUserImport
import net.imagini.common.messaging.serde.VDNAUniversalDeserializer
import net.imagini.dxp.common.{BSPMessage, Edge, Vid}
import org.apache.donut.{FetcherDelta, Fetcher, KafkaRangePartitioner, DonutAppTask}
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

  val snappyProducer = kafkaUtils.createSnappyProducer[KafkaRangePartitioner](numAcks = 0, batchSize = 500)

  override def onShutdown: Unit = {
    snappyProducer.close
  }

  override def awaitingTermination: Unit = {
    println(s"datasync[${counterReceived.get}] => filter[${counterFiltered.get}] => graphstream[${counterProduced.get}] [invalid ${counterInvalid.get}]")
  }

  override protected def createFetcher(topic: String, partition: Int, groupId: String): Fetcher = {
    topic match {
      case "datasync" => new FetcherDelta(this, topic, partition, groupId) {
        override def handleMessage(messageAndOffset: MessageAndOffset): Unit = {
          counterReceived.incrementAndGet
          val payload = messageAndOffset.message.payload
          //FIXME now that we have ByteBuffers vdna decoder should support offset to deserialize from
          val payloadArray: Array[Byte] = util.Arrays.copyOfRange(payload.array, payload.arrayOffset, payload.arrayOffset + payload.remaining)
          val vdnaMsg = vdnaMessageDecoder.decodeBytes(payloadArray)
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
      snappyProducer.send(
        new KeyedMessage(
          "graphstream",
            ByteBuffer.wrap(BSPMessage.encodeKey(vdnaId)),
            ByteBuffer.wrap(BSPMessage.encodePayload((1, Map(partnerId -> edge))))),
        new KeyedMessage(
          "graphstream",
            ByteBuffer.wrap(BSPMessage.encodeKey(partnerId)),
            ByteBuffer.wrap(BSPMessage.encodePayload((1, Map(vdnaId -> edge)))))
      )
      counterProduced.addAndGet(2L)
    } catch {
      case e: IllegalArgumentException => counterInvalid.incrementAndGet
    }
  }

}

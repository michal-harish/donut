package net.imagini.dxp.syncstransform

import java.util
import java.util.concurrent.atomic.AtomicLong

import kafka.message.MessageAndOffset
import kafka.producer.{ProducerConfig, Producer}
import net.imagini.common.message.VDNAUserImport
import net.imagini.common.messaging.serde.VDNAUniversalDeserializer
import net.imagini.dxp.common.{VidPartitioner, BSPMessage, Edge, Vid}
import org.apache.donut.DonutAppTask
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 15/09/15.
 */
class SyncsTransformProcessUnit(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {

  val vdnaMessageDecoder = new VDNAUniversalDeserializer
  val counterReceived = new AtomicLong(0)
  val counterFiltered = new AtomicLong(0)
  val idSpaceSet = Set("a", "r", "d")

//  val producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(new java.util.Properties {
//    put("metadata.broker.list", config.get("kafka.brokers"))
//    put("request.required.acks", "0")
//    put("producer.type", "async")
//    put("serializer.class", classOf[kafka.serializer.DefaultEncoder].getName)
//    put("partitioner.class", classOf[VidPartitioner].getName)
//    put("batch.num.messages", "500")
//    put("compression.codec", "2") //SNAPPY
//  }))

  override def onShutdown: Unit = {
    println(s"Shutting down SyncsTransformProcessUnit ${Thread.currentThread.getId}")
  }

  override def awaitingTermination(avgReadProgress: Float, avgProcessProgress: Float): Unit = {
    println(s"SyncsTransformProcessUnit datasync(${counterReceived.get}) => filter(${counterFiltered.get}) => ... | read: ${avgReadProgress}%, processed: ${avgProcessProgress}")
  }

  override def asyncUpdateState(messageAndOffset: MessageAndOffset): Unit = {
    counterReceived.incrementAndGet
  }

  override def asyncProcessMessage(messageAndOffset: MessageAndOffset): Unit = {
    val payload = messageAndOffset.message.payload
    //FIXME now that we have ByteBuffers vdna decoder should support offset to deserialize from
    val payloadArray: Array[Byte] = util.Arrays.copyOfRange(payload.array, payload.arrayOffset(), payload.arrayOffset + messageAndOffset.message.payloadSize )
    val vdnaMsg = vdnaMessageDecoder.decodeBytes(payloadArray)

    if (vdnaMsg.isInstanceOf[VDNAUserImport]) {
      val importMsg = vdnaMsg.asInstanceOf[VDNAUserImport]
      if (importMsg.getUserCookied &&
        !importMsg.getUserOptOut &&
        importMsg.getUserUid != null &&
        importMsg.getPartnerUserId != null &&
        idSpaceSet.contains(importMsg.getIdSpace)) {
        //transformAndProduce(importMsg)
        counterFiltered.incrementAndGet
      }
    }
  }

//  def transformAndProduce(importMsg: VDNAUserImport) = {
//    val vdnaId = Vid("vdna", importMsg.getUserUid.toString)
//    val partnerId = Vid(importMsg.getIdSpace, importMsg.getPartnerUserId)
//    val edge = Edge("AAT", 1.0, importMsg.getTimestamp)
//    producer.send(
//      new KeyedMessage[Array[Byte], Array[Byte]](
//        "graphstream", BSPMessage.encodeKey(vdnaId), BSPMessage.encodePayload((1, Map(partnerId -> edge)))),
//      new KeyedMessage[Array[Byte], Array[Byte]](
//        "graphstream", BSPMessage.encodeKey(partnerId), BSPMessage.encodePayload((1, Map(vdnaId -> edge))))
//    )
//  }

}

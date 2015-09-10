package net.imagini.dxp.donut

import java.util.concurrent.{Executors, TimeUnit}

import kafka.consumer.KafkaStream
import net.imagini.common.message.VDNAUserImport
import net.imagini.common.messaging.kafka8.IntegerDecoderKafka08
import net.imagini.common.messaging.serde.VDNAUniversalDeserializer
import net.imagini.dxp.common.{Vid, Edge}
import org.apache.donut.{DonutProducer, DonutConsumer}


class SyncsTransformer(zooKeeper: String, brokers: String, numThreads: Int, producer: DonutProducer[GraphMessage]) {

  val executor = Executors.newFixedThreadPool(numThreads)

  val intDecoder = new IntegerDecoderKafka08
  val vdnaMessageDecoder = new VDNAUniversalDeserializer

  val consumer = DonutConsumer(zooKeeper, "SyncsToGraphTransformer")

  def start {
    val streams = consumer.createMessageStreams(Map("datasync" -> numThreads))("datasync")
    for (stream <- streams) {
      executor.submit(new SyncsTransformerThread(stream, "r"))
    }
  }

  def stop {
    try {
      consumer.shutdown
    } finally {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        executor.shutdownNow
      }
      producer.close
    }
  }


  class SyncsTransformerThread(val stream: KafkaStream[Array[Byte], Array[Byte]], idSpaces: String*) extends Runnable {

    val idSpaceSet = idSpaces.toSet

    override def run {
      val it = stream.iterator
      while (it.hasNext) {
        val msgAndMeta = it.next
        val key = intDecoder.fromBytes(msgAndMeta.key)
        val vdnaMsg = vdnaMessageDecoder.decodeBytes(msgAndMeta.message)
        if (vdnaMsg.isInstanceOf[VDNAUserImport]) {
          val importMsg = vdnaMsg.asInstanceOf[VDNAUserImport]
          if (importMsg.getUserCookied &&
            !importMsg.getUserOptOut &&
            importMsg.getUserUid != null &&
            importMsg.getPartnerUserId != null &&
            idSpaceSet.contains(importMsg.getIdSpace)) {
            //println(s"consuming ${importMsg}")
            transformAndProduce(importMsg)
          }
        }
      }
      System.out.println(s"Shutting down ConsumerThread ${Thread.currentThread.getId}"
      )
    }

    def transformAndProduce(importMsg: VDNAUserImport) = {
      val vdnaId = Vid("vdna", importMsg.getUserUid.toString)
      val partnerId = Vid(importMsg.getIdSpace, importMsg.getPartnerUserId)
      val edge = Edge("AAT", 1.0, importMsg.getTimestamp)

      producer.send(List(
        GraphMessage(vdnaId, 1, Map(partnerId -> edge)),
        GraphMessage(partnerId, 1, Map(vdnaId -> edge))))
    }
  }

}




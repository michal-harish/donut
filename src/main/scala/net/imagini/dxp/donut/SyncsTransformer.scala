package net.imagini.dxp.donut

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import kafka.consumer.{ConsumerConfig, Consumer}
import kafka.producer.{KeyedMessage, Producer}
import net.imagini.common.message.VDNAUserImport
import net.imagini.common.messaging.serde.VDNAUniversalDeserializer
import net.imagini.dxp.common.{Vid, Edge}

/**
 * Transformer is defined by f(inMessage) -> outMessage
 * @param zooKeeper
 * @param producer
 * @param idSpaces
 */
class SyncsTransformer(zooKeeper: String, producer: Producer[Array[Byte], Array[Byte]], idSpaces: String*) {

  val vdnaMessageDecoder = new VDNAUniversalDeserializer

  val consumer = Consumer.create(new ConsumerConfig(new java.util.Properties {
    put("zookeeper.connect", zooKeeper)
    put("group.id", "SyncsToGraphTransformer")
    put("zookeeper.session.timeout.ms", "3000")
    put("zookeeper.sync.time.ms", "500")
    put("auto.commit.interval.ms", "1000")
  }))

  val stream = consumer.createMessageStreams(Map("datasync" -> 1))("datasync").head
  val counter1 = new AtomicLong(0)
  val counter2 = new AtomicLong(0)
  val idSpaceSet = idSpaces.toSet
  val executor = Executors.newSingleThreadExecutor

  def stop = {
    try {
      consumer.shutdown
      executor.shutdown
      executor.awaitTermination(10, TimeUnit.SECONDS)
    } finally {
      executor.shutdownNow
    }
  }

  def start = executor.submit(new Runnable() {
    override def run {
      val it = stream.iterator
      while (!Thread.interrupted && it.hasNext) {
        val msgAndMeta = it.next
        val vdnaMsg = vdnaMessageDecoder.decodeBytes(msgAndMeta.message)
        counter1.incrementAndGet
        if (vdnaMsg.isInstanceOf[VDNAUserImport]) {
          val importMsg = vdnaMsg.asInstanceOf[VDNAUserImport]
          if (importMsg.getUserCookied &&
            !importMsg.getUserOptOut &&
            importMsg.getUserUid != null &&
            importMsg.getPartnerUserId != null &&
            idSpaceSet.contains(importMsg.getIdSpace)) {
            transformAndProduce(importMsg)
            counter2.incrementAndGet
          }
        }
      }
      System.out.println(s"Shutting down ConsumerThread ${Thread.currentThread.getId}")
    }

    def transformAndProduce(importMsg: VDNAUserImport) = {
      val vdnaId = Vid("vdna", importMsg.getUserUid.toString)
      val partnerId = Vid(importMsg.getIdSpace, importMsg.getPartnerUserId)
      val edge = Edge("AAT", 1.0, importMsg.getTimestamp)
      producer.send(
        new KeyedMessage[Array[Byte], Array[Byte]](
          "graphstream", BSPMessage.encodeKey(vdnaId), BSPMessage.encodePayload((1, Map(partnerId -> edge)))),
        new KeyedMessage[Array[Byte], Array[Byte]](
          "graphstream", BSPMessage.encodeKey(partnerId), BSPMessage.encodePayload((1, Map(vdnaId -> edge))))
      )
    }
  })

}




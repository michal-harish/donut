package org.apache.donut

import kafka.producer.Producer
import kafka.producer.ProducerConfig


/**
 * Created by mharis on 10/09/15.
 */
case class DonutProducer[M <: DonutMessage[_,_]](brokers: String, batchSize: Int, numAcks: Int)//(implicit k: Manifest[_], v: Manifest[_])
  extends Producer[Array[Byte], Array[Byte]](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", brokers)
    put("request.required.acks", numAcks.toString)
    put("producer.type", "async")
    put("serializer.class", classOf[kafka.serializer.DefaultEncoder].getName)
    put("partitioner.class", classOf[org.apache.donut.DonutPartitioner].getName)
    put("batch.num.messages", batchSize.toString)
    put("compression.codec", "2") //SNAPPY
  })) {

  def send(messages: List[M]) = {
    super.send(messages:_*)
  }


}
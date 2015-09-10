package org.apache.donut

import kafka.producer.Producer
import kafka.producer.ProducerConfig


/**
 * Created by mharis on 10/09/15.
 */
case class DonutProducer[M <: DonutMessage[_,_]](brokers: String)//(implicit k: Manifest[_], v: Manifest[_])
  extends Producer[Array[Byte], Array[Byte]](new ProducerConfig(new java.util.Properties {
    put("metadata.broker.list", brokers)
    put("serializer.class", classOf[kafka.serializer.DefaultEncoder].getName)
    put("partitioner.class", classOf[org.apache.donut.DonutPartitioner].getName)
    put("producer.type", "async")
    put("request.required.acks", "1")
  })) {

  def send(messages: List[M]) = {
    super.send(messages:_*)
  }


}
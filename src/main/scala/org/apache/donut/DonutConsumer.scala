package org.apache.donut

import kafka.consumer.{ConsumerConnector, Consumer, ConsumerConfig}

/**
 * Created by mharis on 10/09/15.
 */
object DonutConsumer {
  def apply(zooKeeperConnect: String, consumerGroupId: String): ConsumerConnector = {
    Consumer.create(new ConsumerConfig(new java.util.Properties {
      put("zookeeper.connect", zooKeeperConnect)
      put("group.id", consumerGroupId)
      put("zookeeper.session.timeout.ms", "400")
      put("zookeeper.sync.time.ms", "200")
      put("auto.commit.interval.ms", "1000")
    }))
  }
}
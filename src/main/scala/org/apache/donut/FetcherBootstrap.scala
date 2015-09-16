package org.apache.donut

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherBootstrap(kafkaUtils: KafkaUtils, topic: String, partition: Int, groupId: String)
  extends Fetcher(kafkaUtils, topic, partition, groupId) {

  override val initialOffset: Long = consumer.getEarliestOffset

}
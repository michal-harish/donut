package org.apache.donut

/**
 * Created by mharis on 16/09/15.
 */
abstract class FetcherOnce(kafkaUtils: KafkaUtils, topic: String, partition: Int, groupId: String)
  extends Fetcher(kafkaUtils, topic, partition, groupId) {
  /**
   * processOffset - persistent offset mark for remembering up to which point was the stream processed
   */
  override val initialOffset: Long = consumer.getOffset


}
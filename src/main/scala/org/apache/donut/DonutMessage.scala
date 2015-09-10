package org.apache.donut

import kafka.producer.KeyedMessage

/**
 * Created by mharis on 10/09/15.
 */
abstract class DonutMessage[K, P](topic: String, key: Array[Byte], payload: Array[Byte])
  extends KeyedMessage[Array[Byte], Array[Byte]](topic, key, payload) {

  def decodePayload: P

  def decodeKey: K

}
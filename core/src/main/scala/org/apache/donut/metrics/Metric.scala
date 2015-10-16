package org.apache.donut.metrics

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Created by mharis on 16/10/15.
 */
abstract class Metric {

  protected var data = new ConcurrentHashMap[Int, String]()

  def get(partition: Int): String = data.get(partition)

  def put(partition: Int, value: String) = data.put(partition, value)

  def get: String = aggregate(data.values.asScala)

  protected def aggregate(values: Iterable[String]): String

}








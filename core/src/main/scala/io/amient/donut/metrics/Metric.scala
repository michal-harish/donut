package io.amient.donut.metrics

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Created by mharis on 16/10/15.
 */
abstract class Metric {

  protected var data = new ConcurrentHashMap[Int, (String, String)]()

  def value(partition: Int): String = if (data.containsKey(partition)) data.get(partition)._1 else null

  def hint(partition: Int): String = if (data.containsKey(partition)) data.get(partition)._2 else ""

  def put(partition: Int, value: String, hint: String) = data.put(partition, (value, hint))

  def value: String = aggregate(data.values.asScala.filter(_._1 != null).map(_._1))

  protected def aggregate(values: Iterable[String]): String

}








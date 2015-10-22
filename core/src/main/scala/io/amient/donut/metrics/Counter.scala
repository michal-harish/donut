package io.amient.donut.metrics

/**
 * Created by mharis on 16/10/15.
 */
class Counter extends Metric {
   override protected def aggregate(values: Iterable[String]): String = values.map(_.toLong).sum.toString
 }
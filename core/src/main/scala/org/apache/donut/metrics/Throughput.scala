package org.apache.donut.metrics

/**
 * Created by mharis on 16/10/15.
 */
class Throughput extends Metric {
   override protected def aggregate(values: Iterable[String]): String = values.map(_.toLong) match {
     case x if (x.size > 0) => x.sum.toString
     case y => "0"
   }
 }

package org.apache.donut.metrics

/**
 * Created by mharis on 16/10/15.
 */
class Status extends Metric {
   override protected def aggregate(values: Iterable[String]): String = "-"
 }

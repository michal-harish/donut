package org.apache.donut.metrics

/**
 * Created by mharis on 16/10/15.
 */
class Progress extends Metric {
  override protected def aggregate(values: Iterable[String]): String = values.map(_.toFloat) match {
    case x if (x.size > 0) => (x.sum / x.size).toString
    case y => "0.0"
  }
}

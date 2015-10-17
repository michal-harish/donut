package org.apache.donut.metrics

/**
 * Created by mharis on 16/10/15.
 */
class Ratio extends Metric {
  override protected def aggregate(values: Iterable[String]): String = {
    val (n,d) = values.map(s => {
      val Array(n,d) = s.split("/")
      (n.toLong, d.toLong)
    }).reduce((a,b) => (a._1 + b._1, a._2 + b._2))
    s"$n/$d"
  }
}

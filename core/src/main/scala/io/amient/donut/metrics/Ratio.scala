/**
 * Donut - Recursive Stream Processing Framework
 * Copyright (C) 2015 Michal Harish
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.amient.donut.metrics

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

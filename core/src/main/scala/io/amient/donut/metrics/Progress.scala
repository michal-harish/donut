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
class Progress extends Metric {
  override protected def aggregate(values: Iterable[String]): String = values
    .filter(_ != "NaN").map(_.toFloat) match {
      case x if (x.size > 0) => (x.sum / x.size).toString
      case y => "0.0"
    }
}


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








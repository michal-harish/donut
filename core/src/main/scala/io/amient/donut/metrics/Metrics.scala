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
 * Created by mharis on 19/10/15.
 */
object Metrics {

  val INPUT_PROGRESS = "input progress"
  val CONTAINER_MEMORY = "container:memory"
  val CONTAINER_CORES = "container:cores"
  val CONTAINER_ID = "container:id"
  val CONTAINER_LOGS = "container:logs"
  val LAST_ERROR = "container:last-error"
  val LAST_EXIT_STATUS = "container:last-exit-status"
}

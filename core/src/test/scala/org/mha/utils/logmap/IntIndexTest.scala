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

package org.mha.utils.logmap

import org.scalatest.FlatSpec
import org.scalatest.Matchers

/**
 * Created by mharis on 01/10/15.
 */
class IntIndexTest extends FlatSpec with Matchers {

  val index = new IntIndex(65)

  index.capacityInBytes should be (0)

  (0 to 15).foreach(i => {
    index.put(1) should be (i)
    index.capacityInBytes should be(65)
    index.get(i) should be(1)
  })

  (16 to 31).foreach(i => {
    index.put(2) should be (i)
    index.capacityInBytes should be(130)
    index.get(i) should be(2)
  })

  (0 to 31).foreach(i => {
    index.get(i) should be (i / 16 + 1)
  })
}

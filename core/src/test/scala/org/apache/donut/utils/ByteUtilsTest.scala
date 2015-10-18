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

package org.apache.donut.utils

import org.mha.utils.ByteUtils
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by mharis on 31/07/15.
  */
class ByteUtilsTest extends FlatSpec with Matchers {

   val input = "----020ac416f90d91cffc09b56a9e7aea0420e0cf59----"

   val b = ByteUtils.parseRadix16(input.getBytes, 4, 40)

   ByteUtils.toRadix16(b, 0, 20) should be ("020ac416f90d91cffc09b56a9e7aea0420e0cf59")
 }

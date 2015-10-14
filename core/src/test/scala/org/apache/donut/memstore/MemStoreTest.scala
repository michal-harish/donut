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

package org.apache.donut.memstore

import java.nio.ByteBuffer

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 14/09/15.
 */
class MemStoreTest extends FlatSpec with Matchers {

  implicit def stringAsBytes(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes)

  behavior of "MemStoreLog"
  it should "behave as expected" in {
    val st = new MemStoreLogMap(maxSizeInMb = 1024, segmentSizeMb = 1)
    test(st)
    st.size should be(5)
  }

//  behavior of "MemStoreMemDb"
//  it should "behave as expected" in {
//    val st = new MemStoreMemDb(1024)
//    test(st)
//    st.size should be(5)
//  }

  def test(storage: MemStore) = {

    storage.put("9000", "A")
    storage.put("8000", "B")
    storage.put(ByteBuffer.wrap("7000".getBytes), ByteBuffer.wrap("C".getBytes))
    // after 3 puts the order of eviction is 7,8,(9)
    storage.size should be(3)

    storage.get("9000") // refreshes 9 so the order of eviction is 9,7,(8)
    storage.put("6000", "D") // pushes out 8
    storage.put("5000", null) // pushes out 7


    storage.contains("9000") should be(true)
    storage.contains("6000") should be(true)
    storage.contains("5000") should be(true)
    storage.get("5000") should be (Some(null))
  }


}

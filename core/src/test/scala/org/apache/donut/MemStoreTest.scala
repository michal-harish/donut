package org.apache.donut

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

import java.nio.ByteBuffer

import org.apache.donut.memstore.{MemStoreMemDb, MemStoreDumb, MemStore}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by mharis on 14/09/15.
 */
class MemStoreTest extends FlatSpec with Matchers {

  implicit def stringAsBytes(s: String): Array[Byte] = s.getBytes

  behavior of "Dumb LocalStorage1"
  it should "behave as expected" in {
    val st = new MemStoreDumb(3)
    test(st)
    st.size should be(3)
    st.contains("8") should be(false)
    st.contains("7") should be(false)
  }

  behavior of "Fancy LocalStorage2"
  it should "behave as expected" in {
    test(new MemStoreMemDb(1024, 2))
  }

  def test(storage: MemStore) = {

    storage.put("9", "A")
    storage.put("8", "B")
    storage.put(ByteBuffer.wrap("7".getBytes), ByteBuffer.wrap("C".getBytes))
    // after 3 puts the order of eviction is 7,8,(9)
    storage.size should be(3)

    storage.get("9") // refreshes 9 so the order of eviction is 9,7,(8)
    storage.put("6", "D") // pushes out 8
    storage.put("5", null) // pushes out 7


    storage.contains("9") should be(true)
    storage.contains("6") should be(true)
    storage.contains("5") should be(true)
    storage.get("5") should be (Some(null))
  }


}

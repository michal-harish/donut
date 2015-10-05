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

package org.apache.donut

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Future, TimeUnit, TimeoutException}

/**
 * Created by mharis on 11/09/15.
 */
trait KeyValueStoreClient {
  private val UNSET = Array[Byte]()

  def asyncGet(key: Array[Byte], timeoutMs: Long, callback: (Array[Byte]) => Unit): Unit //down to queuing implementation

  final def get(key: Array[Byte], timeoutMs: Long = -1L) = futureGet(key, timeoutMs).get

  final def futureGet(key: Array[Byte], timeoutMs: Long = -1L): Future[Array[Byte]] = {
    new Future[Array[Byte]]() {
      val result = new AtomicReference[Array[Byte]](UNSET)
      asyncGet(key, timeoutMs, (value: Array[Byte]) => result.synchronized {
        result.set(value)
        result.notify
      })
      override def isDone: Boolean = !result.get.equals(UNSET)
      override def get(): Array[Byte] = get(timeoutMs, TimeUnit.MILLISECONDS)
      override def get(timeout: Long, unit: TimeUnit): Array[Byte] = {
        if (!isDone) result.synchronized(if (timeout>0) result.wait(unit.toMillis(timeout)) else result.wait)
        result.get match {
          case value: Array[Byte] if (value.equals(UNSET)) => throw new TimeoutException
          case any: Any => any
        }
      }

      override def isCancelled: Boolean = ???

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = ???
    }
  }
}

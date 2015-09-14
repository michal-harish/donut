package net.imagini.dxp.donut

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

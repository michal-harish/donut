package org.apache.donut

import java.lang.reflect.Constructor
import java.util.concurrent.{TimeUnit, Executors}
import org.apache.hadoop.conf.Configuration
import org.apache.yarn1.YarnClient
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Created by mharis on 14/09/15.
 * The DonutApp must be extended by the application and then either runOnYarn or runLocally
 */
class DonutApp[T <: DonutAppTask](puMemoryMb: Int, cogroup: Boolean, topics: String*)(implicit t: ClassTag[T]) {
  private val log = LoggerFactory.getLogger(classOf[DonutApp[_]])

  val taskClass: Class[T] = t.runtimeClass.asInstanceOf[Class[T]]
  val taskPriority = 0

  def runOnYarn(conf: Configuration): Unit = {
    try {
      val numLogicalPartitions = KafkaUtils(conf).getNumLogicalPartitions(topics, cogroup)
      val args: Array[String] = Array(taskClass.getName,
        numLogicalPartitions.toString, taskPriority.toString, puMemoryMb.toString) ++ topics
      YarnClient.submitApplicationMaster(conf, true, classOf[DonutYarnAM], args)
    } catch {
      case e: Throwable => {
        log.error("", e)
        System.exit(1)
      }
    }
  }

  def runLocally(conf: Configuration): Unit = {
    try {
      val numLogicalPartitions = 1 // KafkaUtils(config).getNumLogicalPartitions(topics, cogroup)
      val taskConstructor: Constructor[T] = taskClass.getConstructor(
        classOf[Configuration], classOf[Int], classOf[Int], classOf[Seq[String]])
      val executor = Executors.newFixedThreadPool(numLogicalPartitions)
      (0 to numLogicalPartitions - 1).foreach(lp => {
        executor.submit(taskConstructor.newInstance(conf, new Integer(lp), new Integer(numLogicalPartitions), topics))
      })
      executor.shutdown
      while (true) {
        if (executor.awaitTermination(10, TimeUnit.SECONDS)) {
          System.exit(0)
        }
      }
    } catch {
      case e: Throwable => {
        log.error("", e)
        System.exit(1)
      }
    }
  }

}


package org.apache.donut

import java.lang.reflect.Constructor
import java.util.Properties

import org.apache.yarn1.YarnClient

/**
 * Created by mharis on 14/09/15.
 */
object DonutYarnContainer {
  def main(args: Array[String]): Unit = {
    try {
      val taskClass = Class.forName(args(0)).asInstanceOf[Class[DonutAppTask]]
      val taskConstructor: Constructor[DonutAppTask] = taskClass.getConstructor(
          classOf[Properties], classOf[Int], classOf[Int], classOf[Seq[String]])
      taskConstructor.newInstance(
        YarnClient.getAppConfiguration, Integer.valueOf(args(1)), Integer.valueOf(args(2)), (3 to args.length-1).map(args(_))
      ).run
    } catch {
      case e: Throwable => {
        e.printStackTrace(System.out)
        System.exit(1)
      }
    }
  }
}
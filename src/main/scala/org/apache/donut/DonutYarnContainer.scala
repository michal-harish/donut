package org.apache.donut

import java.lang.reflect.Constructor

import org.apache.hadoop.conf.Configuration
import org.apache.yarn1.Yarn1Configuration

/**
 * Created by mharis on 14/09/15.
 */
object DonutYarnContainer {
  def main(args: Array[String]): Unit = {
    try {
      val taskClass = Class.forName(args(0)).asInstanceOf[Class[DonutAppTask]]
      val taskConstructor: Constructor[DonutAppTask] = taskClass.getConstructor(
          classOf[Configuration], classOf[Int], classOf[Int], classOf[Seq[String]])
      taskConstructor.newInstance(
        new Yarn1Configuration(), Integer.valueOf(args(1)), Integer.valueOf(args(2)), (3 to args.length-1).map(args(_))
      ).run
    } catch {
      case e: Throwable => {
        e.printStackTrace(System.out)
        System.exit(1)
      }
    }
  }
}

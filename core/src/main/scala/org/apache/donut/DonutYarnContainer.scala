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

import java.lang.reflect.Constructor
import java.net.URL
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
        classOf[Properties],
        classOf[URL],
        classOf[Int],
        classOf[Int],
        classOf[Seq[String]])

      val taskInstance = taskConstructor.newInstance(
        YarnClient.getAppConfiguration, //conf
        new URL(args(1)), //master url
        Integer.valueOf(args(2)), //logicalPartition
        Integer.valueOf(args(3)), //numLogicalPartitions
        (4 to args.length - 1).map(args(_))) //followed by application arguments

      try {
        taskInstance.run
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          System.exit(22)
        }
      }

    } catch {
      case e: Throwable => {
        //TODO create UI interface and separate it from DonutAppTask so that we can also show errors when
        //we don't have instance of task yet
        e.printStackTrace()
        Thread.sleep(15000)
        System.exit(21)
      }
    }
  }
}

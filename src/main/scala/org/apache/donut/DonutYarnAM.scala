package org.apache.donut

import org.apache.yarn1.{YarnContainerRequest, YarnMaster}

/**
 * Created by mharis on 14/09/15.
 */
final class DonutYarnAM extends YarnMaster {
  var topics: Seq[String] = null
  var numLogicalPartitions = -1
  final protected override def onStartUp(args: Array[String]): Unit = {
    val taskClass = Class.forName(args(0))
    numLogicalPartitions = args(1).toInt
    val taskPriority = args(2).toInt
    val taskMemoryMb = args(3).toInt
    topics = (4 to args.length - 1).map(args(_)).toSeq
    requestContainerGroup((0 to numLogicalPartitions - 1).map(lp => {
      val args: Array[String] = Array(taskClass.getName, lp.toString, numLogicalPartitions.toString) ++ topics
      new YarnContainerRequest(DonutYarnContainer.getClass, args, taskPriority, taskMemoryMb, 1)
    }).toArray)
  }

  final protected override def getProgress: Float  = {
    println(config.get("kafka.brokers"))
    println(config.get("kafka.group.id"))
    println(s"numLogicalPartitions ${numLogicalPartitions}, topics = ${topics}")
    //TODO return averageReadProgress for all consumer groups -> hence the group Id must be known and derived from yarn.name
    0.0f

  }
}


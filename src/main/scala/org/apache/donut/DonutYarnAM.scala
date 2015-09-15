package org.apache.donut

import org.apache.yarn1.{YarnContainerRequest, YarnMaster}

/**
 * Created by mharis on 14/09/15.
 */
final class DonutYarnAM extends YarnMaster {
  var topics: Seq[String] = null
  var numLogicalPartitions = -1
  var lastProgress: (Long, Float, Float) = (-1, 0f, 0f)

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
    if (lastProgress._1 + 10000L < System.currentTimeMillis) {
      println(config.get("kafka.brokers"))
      println(config.get("kafka.group.id"))
      println(s"numLogicalPartitions ${numLogicalPartitions}, topics = ${topics}")
      //TODO return averageReadProgress for all consumer groups -> hence the group Id must be known and derived from yarn.name
      //    val (readProgress, processProgress) = fetchers.map(_.progress).reduce((f1,f2) => (f1._1 + f2._1, f1._2 + f2._2))
      //    (readProgress / fetchers.size, processProgress / fetchers.size)
      //--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
      //    def progress: (Float, Float) = {
      //      val currentEarliestOffset = consumer.getEarliestOffset
      //      val currentLatestOffset = consumer.getLatestOffset
      //      val availableOffsets = currentLatestOffset - currentEarliestOffset
      //      val processProgress = 100f * (processOffset - currentEarliestOffset) / availableOffsets
      //      val readProgress = 100f * (readOffset - currentEarliestOffset) / availableOffsets
      //      (readProgress, processProgress)
      //    }
      val readProgress = 0f
      val processProgress = 0f
      lastProgress = (System.currentTimeMillis, readProgress, processProgress)
    }
    lastProgress._2
  }
}


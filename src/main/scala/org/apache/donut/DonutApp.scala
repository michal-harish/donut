package org.apache.donut

import java.lang.reflect.Constructor
import java.util.concurrent.{TimeUnit, Executors}
import org.apache.hadoop.conf.Configuration
import org.apache.yarn1.{YarnContainerRequest, YarnMaster, YarnClient}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Created by mharis on 14/09/15.
 * The DonutApp must be extended by the application and then either runOnYarn or runLocally
 */
class DonutApp[T <: DonutAppTask](config: Configuration)(implicit t: ClassTag[T]) extends YarnMaster(config) {

  private val log = LoggerFactory.getLogger(classOf[DonutApp[_]])

  val taskClass: Class[T] = t.runtimeClass.asInstanceOf[Class[T]]
  val kafkaUtils: KafkaUtils = new KafkaUtils(config)
  val topics = config.get("kafka.topics").split(",").toSeq
  val groupId = config.get("kafka.group.id")

  var numLogicalPartitions = -1
  var lastProgress: (Long, Float) = (-1, 0f)
  val updateFrequencyMs = TimeUnit.SECONDS.toMillis(30)

  final def runLocally(multiThreadMode: Boolean): Unit = {
    try {
      numLogicalPartitions = if (!multiThreadMode) 1 else KafkaUtils(config).getNumLogicalPartitions(topics)
      val taskConstructor: Constructor[T] = taskClass.getConstructor(
        classOf[Configuration], classOf[Int], classOf[Int], classOf[Seq[String]])
      val executor = Executors.newFixedThreadPool(numLogicalPartitions)
      (0 to numLogicalPartitions - 1).foreach(lp => {
        executor.submit(taskConstructor.newInstance(config, new Integer(lp), new Integer(numLogicalPartitions), topics))
      })
      executor.shutdown
      while (true) {
        if (executor.awaitTermination(updateFrequencyMs, TimeUnit.MILLISECONDS)) {
          System.exit(0)
        }
        println(s"Progress $getProgress")
      }
    } catch {
      case e: Throwable => {
        log.error("", e)
        System.exit(1)
      }
    }
  }


  final def runOnYarn(puMemoryMb: Int): Unit = {
    try {
      val args: Array[String] = Array(puMemoryMb.toString, "0")
      YarnClient.submitApplicationMaster(config, true, this.getClass, args)
    } catch {
      case e: Throwable => {
        log.error("", e)
        System.exit(1)
      }
    }
  }

  /**
   * Yarn Master Startup hook
   */
  final protected override def onStartUp(args: Array[String]): Unit = {
    numLogicalPartitions = kafkaUtils.getNumLogicalPartitions(topics)
    val taskMemoryMb = args(0).toInt
    val taskPriority = args(1).toInt
    requestContainerGroup((0 to numLogicalPartitions - 1).map(lp => {
      val args: Array[String] = Array(taskClass.getName, lp.toString, numLogicalPartitions.toString) ++ topics
      new YarnContainerRequest(DonutYarnContainer.getClass, args, taskPriority, taskMemoryMb, 1)
    }).toArray)
  }

  /**
   * Instead of reporting progress as ratio of completed tasks which is more suitable for finite jobs,
   * in streaming progress can be seen as an inverse of lag
   * @return average percentage representation of how the underlying fetchers are caught up with available data.
   */
  final protected override def getProgress: Float = {
    if (lastProgress._1 + updateFrequencyMs < System.currentTimeMillis) {
      println(s"numLogicalPartitions ${numLogicalPartitions}, topics = ${topics} in group {$groupId}")
      val offsets: Seq[(Long, Long, Long)] = kafkaUtils.getPartitionMap(topics).flatMap {
        case (topic, numPartitions) => (0 to numPartitions - 1).map(partition => {
          val leader = kafkaUtils.findLeader(topic, partition)
          val consumer = new kafkaUtils.PartitionConsumer(topic, partition, leader, 10000, 64 * 1024, groupId)
          var result = (-1L, -1L, -1L)
          try {
            result = (consumer.getEarliestOffset, consumer.getOffset, consumer.getLatestOffset)
          } finally {
            consumer.close
          }
          result
        })
      }.toSeq

      val progress: Seq[Float] = offsets.map { case (earliest, consumed, latest) => {
        val availableOffsets = latest - earliest
        100f * (consumed - earliest) / availableOffsets
      }
      }
      val avgProgress = progress.sum / progress.size
      lastProgress = (System.currentTimeMillis, avgProgress)
    }
    lastProgress._2
  }

}


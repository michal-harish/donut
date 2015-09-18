import net.imagini.dxp.common.{BSPMessage, VdnaClusterConfig}
import net.imagini.dxp.graphbsp.GraphStreamApplication
import net.imagini.dxp.syncstransform.SyncsTransformApplication
import org.apache.donut.KafkaUtils

/**
 * Created by mharis on 14/09/15.
 */
object GraphStreamLocalLauncher extends App {
  new GraphStreamApplication().runLocally(multiThreadMode = false)
}

object GraphStreamYarnLauncher extends App {
  new GraphStreamApplication().runOnYarn(20 * 1024, awaitCompletion = true)
}

object SyncTransformLocalLauncher extends App {
  new SyncsTransformApplication().runLocally(multiThreadMode = false)
}

object SyncTransformYarnLauncher extends App {
  new SyncsTransformApplication().runOnYarn(taskMemoryMb = 16 * 1024, awaitCompletion = true )
  //FIXME json deserializers kill memory - even 12 x 3GBs (!) will kill container but this is a simple transformation 256Mb should be enough
}

object SyncTransformYarnSubmit extends App {
  new SyncsTransformApplication().runOnYarn(taskMemoryMb = 4 * 1024, awaitCompletion = false)
}

object GraphStreamDebugger extends App {
  val config = new VdnaClusterConfig
  val kafkaUtils = new KafkaUtils(config)
  kafkaUtils.createDebugConsumer("graphstream", (msg) => {
    val vid = BSPMessage.decodeKey(msg.key)
    val payload = msg.message match {
      case null => null
      case x => BSPMessage.decodePayload(x)
    }
    if (payload._2.size > 0) {
      println(s"${vid} -> ${payload}")
    }
  })
}

object GraphStateDebugger extends App {
  val config = new VdnaClusterConfig
  val kafkaUtils = new KafkaUtils(config)
  kafkaUtils.createDebugConsumer("graphstate", (msg) => {
    val vid = BSPMessage.decodeKey(msg.key)
    val payload = msg.message match {
      case null => null
      case x => BSPMessage.decodePayload(x)
    }
    if (payload._2.size > 3) {
      println(s"${vid} -> ${payload}")
    }
  })
}

object OffsetManager extends App {

  val config = new VdnaClusterConfig

  val kafkaUtils = new KafkaUtils(config)

  kafkaUtils.getPartitionMap(Seq("datasync")).foreach { case (topic, numPartitions) => {
    for (p <- (0 to numPartitions - 1)) {
      val consumer = new kafkaUtils.PartitionConsumer(topic, p, "SyncsToGraphTransformer")

      val (earliest, consumed, latest) = (consumer.getEarliestOffset, consumer.getOffset, consumer.getLatestOffset)

      consumer.commitOffset(latest)

      val reset = consumer.getOffset

      consumer.close

      println(s"$topic/$p OFFSET RANGE = ${earliest}:${latest} => DonutTestConsumer offset ${consumed} => ${reset}")
    }
  }}

}

import net.imagini.dxp.common.BSPMessage
import net.imagini.dxp.graphbsp.GraphStreamingBSP
import net.imagini.dxp.syncstransform.GraphSyncsStreamingTransform
import org.apache.donut.KafkaUtils
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 14/09/15.
 */
object GraphStreamLocalLauncher extends App {
  new GraphStreamingBSP().runLocally(multiThreadMode = false)
}

object GraphStreamYarnLauncher extends App {
  new GraphStreamingBSP().runOnYarn(20 * 1024, awaitCompletion = true)
}


object GraphStreamYarnSubmit extends App {
  new GraphStreamingBSP().runOnYarn(20 * 1024, awaitCompletion = false)
}

object SyncTransformLocalLauncher extends App {
  new GraphSyncsStreamingTransform().runLocally(multiThreadMode = false)
}

object SyncTransformYarnLauncher extends App {
  new GraphSyncsStreamingTransform().runOnYarn(taskMemoryMb = 16 * 1024, awaitCompletion = true )
  //FIXME json deserializers kill memory - even 12 x 3GBs (!) will kill container but this is a simple transformation 256Mb should be enough
}

object SyncTransformYarnSubmit extends App {
  new GraphSyncsStreamingTransform().runOnYarn(taskMemoryMb = 4 * 1024, awaitCompletion = false)
}

object GraphStreamDebugger extends App {
  val config = new Configuration { addResource("/etc/vdna/graphstream/config.properties") }
  val kafkaUtils = new KafkaUtils(config)
  kafkaUtils.createDebugConsumer("graphstream", (msg) => {
    val vid = BSPMessage.decodeKey(msg.key)
    val payload = msg.message match {
      case null => null
      case x => BSPMessage.decodePayload(x)
    }
    if (payload != null && payload._2.size > 0) {
      println(s"${vid} -> ${payload}")
    }
  })
}

object GraphStateDebugger extends App {
  val config = new Configuration { addResource("/etc/vdna/graphstream/config.properties") }
  val kafkaUtils = new KafkaUtils(config)
  kafkaUtils.createDebugConsumer("graphstate", (msg) => {
    val vid = BSPMessage.decodeKey(msg.key)
    val payload = msg.message match {
      case null => null
      case x => BSPMessage.decodePayload(x)
    }
    if (payload != null && payload._2.size > 1) {
      print(s"\n${vid} -> ${payload}")
    }
  })
}

object OffsetReport extends App {

  val config = new Configuration { addResource("/etc/vdna/graphstream/config.properties") }

  val kafkaUtils = new KafkaUtils(config)

  kafkaUtils.getPartitionMap(Seq("datasync")).foreach { case (topic, numPartitions) => {
    List("GraphStreamingBSP", "").foreach(consumerGroupId => {
      for (p <- (0 to numPartitions - 1)) {
        val consumer = new kafkaUtils.PartitionConsumer(topic, p, consumerGroupId)

        val (earliest, consumed, latest) = (consumer.getEarliestOffset, consumer.getOffset, consumer.getLatestOffset)

        println(s"$topic/$p OFFSET RANGE = ${earliest}:${latest} => ${consumerGroupId} group offset ${consumed} }")
      }
    })
  }}
}

import java.io.FileInputStream
import java.util.Properties

import net.imagini.graphstream.common.BSPMessage
import net.imagini.graphstream.connectedbsp.ConnectedBSP
import net.imagini.graphstream.syncstransform.SyncsToGraph
import org.apache.donut.KafkaUtils

/**
 * Created by mharis on 14/09/15.
 */
object GraphStreamLocalLauncher extends App {
  new ConnectedBSP().runLocally(multiThreadMode = false)
}

object GraphStreamYarnLauncher extends App {
  new ConnectedBSP().runOnYarn(20 * 1024, awaitCompletion = true)
}


object GraphStreamYarnSubmit extends App {
  new ConnectedBSP().runOnYarn(20 * 1024, awaitCompletion = false)
}

object SyncTransformLocalLauncher extends App {
  new SyncsToGraph().runLocally(multiThreadMode = false)
}

object SyncTransformYarnLauncher extends App {
  new SyncsToGraph().runOnYarn(taskMemoryMb = 16 * 1024, awaitCompletion = true )
  //FIXME json deserializers kill memory - even 12 x 3GBs (!) will kill container but this is a simple transformation 256Mb should be enough
}

object SyncTransformYarnSubmit extends App {
  new SyncsToGraph().runOnYarn(taskMemoryMb = 4 * 1024, awaitCompletion = false)
}

object Config extends Properties {
  load( new FileInputStream("/etc/vdna/graphstream/config.properties"))
}
object GraphStreamDebugger extends App {
  val kafkaUtils = new KafkaUtils(Config)
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
  val kafkaUtils = new KafkaUtils(Config)
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

  val kafkaUtils = new KafkaUtils(Config)

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

import net.imagini.dxp.common.VdnaClusterConfig
import net.imagini.dxp.graphbsp.{GraphStreamApplication, GraphStreamYarnApplication}
import net.imagini.dxp.syncstransform.SyncsTransformApplication
import org.apache.donut.KafkaUtils

/**
 * Created by mharis on 14/09/15.
 */
object GraphStreamLocalLauncher extends App {
  new GraphStreamApplication().runLocally
}

object GraphStreamYarnLauncher extends App {
  GraphStreamYarnApplication.main(args)
}

object SyncTransformLocalLauncher extends App {
  new SyncsTransformApplication().runLocally
}

object SyncTransformYarnLauncher extends App {
  new SyncsTransformApplication().runOnYarn
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

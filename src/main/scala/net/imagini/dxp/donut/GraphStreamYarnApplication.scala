package net.imagini.dxp.donut

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import net.imagini.dxp.common.{Edge, Vid}
import org.apache.donut.{LocalStorage, DonutApp, DonutAppTask, DonutProducer}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable

/**
 * Created by mharis on 10/09/15.
 */
object GraphStreamYarnApplication extends App {

  new GraphStreamDonutApp().runOnYarn
}

class GraphStreamDonutApp extends DonutApp[GraphStreamTask](false, "graphstream") {
  val conf: Configuration = new Configuration

  conf.set("yarn.name", "GraphStream")
  conf.set("yarn.queue", "developers")
  conf.setInt("yarn.master.priority", 0)
  conf.setLong("yarn.master.timeout.s", 3600L)
  conf.addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/core-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new FileInputStream("/opt/envs/prod/etc/hadoop/yarn-site.xml"))
  conf.set("yarn.classpath", "/opt/scala-2.10.4/lib/scala-library.jar")
  conf.set("donut.zookeeper.connect", "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com")
  conf.set("donut.kafka.brokers", "message-01.prod.visualdna.com,message-02.prod.visualdna.com,message-03.prod.visualdna.com")
  conf.set("donut.kafka.port", "9092")

  def runOnYarn: Unit = runOnYarn(conf)
  def runLocally: Unit = runLocally(conf)
}

class GraphStreamTask(config: Configuration, logicalPartition: Int, totalLogicalPartitions: Int, topics: Seq[String])
  extends DonutAppTask(config, logicalPartition, totalLogicalPartitions, topics) {


  val zkHosts = config.get("donut.zookeeper.connect")
  val brokers = config.get("donut.kafka.brokers")
  val kafkaPort = config.get("donut.kafka.port")
  val signal = new Object
  //val producer = DonutProducer[GraphMessage](brokers, batchSize = 1000, numAcks = 0)
  //val transformer = new SyncsTransformer(zkHosts, producer, "r", "d", "a")
  //transformer.start

  var lastProcessedOffset = -1L

  val MAX_ITER = 5
  val MAX_EDGES = 2
  val localState = new LocalStorage[mutable.Set[Vid]](500000)
  val counterReceived = new AtomicLong(0)
  val counterEvicted = new AtomicLong(0)
  val counterInitialised = new AtomicLong(0)
  val counterUpdated = new AtomicLong(0)

  override def onShutdown: Unit = {
    //transformer.stop
    //producer.close
  }

  override def awaitingTermination {
    println(
      //s"datasyncs(${transformer.counter1.get}) " +
      //s"=> transform(${transformer.counter2.get}) " //+
      s"=> graphstream(${counterReceived.get}) - evicted(${counterEvicted.get}}) => (${counterInitialised.get} + ${counterUpdated.get})) " +
      s"=> state.size = " + localState.size
    )
  }

  override def asyncProcessMessage(messageAndOffset: kafka.message.MessageAndOffset): Unit = {
    val msgOffset = messageAndOffset.offset
    counterReceived.incrementAndGet
    val key = messageAndOffset.message.key
    localState.get(key) match {
      case Some(null) => {
        counterEvicted.incrementAndGet
        //this key has been evicted and we remember it
      }
      case None => {
        counterInitialised.incrementAndGet
        val (iteration, inputEdges) = GraphMessage(messageAndOffset).decodePayload
        initState(key, inputEdges)
      }
      case Some(state) => {
        counterUpdated.incrementAndGet
        val (iteration, inputEdges) = GraphMessage(messageAndOffset).decodePayload
        try {
          if (msgOffset > lastProcessedOffset) {
            process(state, iteration, inputEdges)
            lastProcessedOffset = msgOffset
          }
        } finally {
          updateState(key, state, inputEdges)
        }
      }

    }

    def process(state: mutable.Set[Vid], iteration: Int, inputEdges: Map[Vid, Edge]): Unit = {
      if (iteration < MAX_ITER) {
        val additionalEdges = if (state == null) inputEdges else inputEdges.filter(n => !state.contains(n._1))
        //producer.send(state.map(e => GraphMessage(e, iteration + 1, additionalEdges)).toList)
      }
    }


    def initState(key: ByteBuffer, inputEdges: Map[Vid, Edge]): Unit = {
      if (inputEdges.size > MAX_EDGES) {
        localState.put(key, null)
      } else {
        localState.put(key, mutable.Set(inputEdges.map(_._1).toSeq: _*))
      }
    }

    def updateState(key: ByteBuffer, state: mutable.Set[Vid], inputEdges: Map[Vid, Edge]): Unit = {
      val newEdges = inputEdges.filter(n => !state.contains(n._1))
      if (newEdges.size + state.size > MAX_EDGES) {
        localState.put(key, null)
      } else {
        state ++= newEdges.map(_._1)
      }
    }
  }
}


package io.amient.donut.ui

import java.util.concurrent.ConcurrentSkipListMap

import io.amient.donut.metrics._

import scala.collection.JavaConverters._

/**
 * Created by mharis on 17/10/15.
 */

abstract class WebUIServer(val host: String, val port: Int) {

  private val metrics = new ConcurrentSkipListMap[String, Metric]()

  @volatile protected var numLogicalPartitions = 0
  @volatile protected var appClass = "<undefined>"
  @volatile protected var kafkaBrokers = "<undefined>"
  @volatile protected var kafkaGroupId = "<undefined>"
  @volatile protected var topics = List[String]()
  @volatile protected var masterMemoryMb = 0
  @volatile protected var masterCores = 0
  @volatile protected var totalMainMemoryMb = 0L
  @volatile protected var taskOverheadMemMb = 0L

  def start

  def getListeningPort: Int

  def stop

  def getProgress: Float = {
    metrics.get(Metrics.INPUT_PROGRESS) match {
      case null => 0f
      case metric: Progress => metric.value.toFloat
    }
  }

  final protected def handle(method: String, uri: String, params: Map[String, String]): (Int, String, String) = {
    try {
      uri match {
        case "" | "/" => (200, "text/html", serveMain())
        case "/attribute" if (method == "POST") => handlePostAttributes(params)
        case "/metrics" if (method == "POST") => handlePostMetrics(params)
        case uri => (404, "text/html", s"<em>Not Found<em><pre>${uri}</pre>")
      }
    } catch {
      case e: IllegalArgumentException => (400, "text/plain", e.getMessage)
      case i: Throwable => {
        i.printStackTrace()
        (500, "text/plain", i.getStackTraceString)
      }
    }
  }

  private def handlePostAttributes(params: Map[String, String]): (Int, String, String) = {
    params.foreach { case (attr, value) => attr match {
      case "numLogicalPartitions" => numLogicalPartitions = value.toInt
      case "appClass" => appClass = value
      case "topics" => topics = value.split(",").toList
      case "kafkaBrokers" => kafkaBrokers = value
      case "kafkaGroupId" => kafkaGroupId = value
      case "masterMemoryMb" => masterMemoryMb = value.toInt
      case "masterCores" => masterCores = value.toInt
      case "totalMainMemoryMb" => totalMainMemoryMb = value.toLong
      case "taskOverheadMemMb" => taskOverheadMemMb = value.toLong
    }
    }
    (202, "text/plain", "")
  }


  private def handlePostMetrics(params: Map[String, String]): (Int, String, String) = {
    val partition = params("p").toInt
    val name: String = params("n")
    val value: String = params("v")
    val hint: String = params("h")
    val cls = Class.forName(params("c")).asSubclass(classOf[Metric])
    if (!metrics.containsKey(name)) metrics.put(name, cls.newInstance)
    val metric: Metric = metrics.get(name)
    if (!cls.isInstance(metric)) {
      throw new IllegalArgumentException(s"Metric ${name} expected to be of type ${metric.getClass}")
    }
    //log.debug(s"RECEIVED METRIC PARTITION=${partition} NAME=${name} VALUE = ${value} ${hint}")
    metric.put(partition, value, hint)
    (202, "text/plain", "")
  }

  private def serveMain(): String = {
    val activePartitions = (0 to numLogicalPartitions - 1).filter(p => metrics.asScala.exists(_._2.value(p) != null))
    return "<!DOCTYPE html><html lang=\"en\">" +
      "<head>" +
      "<meta charset=\"utf-8\" />" +
      "<link rel=\"icon\" href=\"data:image/ico;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABmJLR0QAAAAAAAD5Q7t/AAAACXBIWXMAAABIAAAASABGyWs+AAAACXZwQWcAAAAQAAAAEABcxq3DAAADjklEQVQ4yzWTTWwUZQCG32/mm9md3dmdbncLpfQH2mJp+v9DGgQbYgVDYjTqATVEDeIJEg4eJMaDMR6M8eTFiEkTExL1wMGgFCMpCtIKKUKLpb+EZel2222X7c7szv98nwf0uT/P7SH4j5nVPE4VU3g3Uqm56fgDs77QUyORjnUPaZPxqRQhUy/7T1anEcJo347/NRAAOLu0hV0Spxct4fW1gJyWOe+1iRDRBCIA4Cs+t0zGlyjn53tC/PuLy2J2rMfF0aYkyBcPt9BIiXpODz40Qc8MyEKsTSIw/QAuJ1BFAhAg53Ncd8DWfDbdRvlHlztOXP5y/huQ0fnp8A/ejs9mA3rmsELpgCzgSsnFVKkCIxChUqBDlXEoHgIVRFwyGR44fmYPZW/dNpwbYv3pT/dNuPh8tyTFuiSCG5aPKxs2Srk8nPUsLKOMBy6gy2HsjxAMhBh8P9BWKmbLsTj/jU7q/rDGWHW/ZKGOS5gzbUgewDcyeCb9J4LAQWbvAdTVdKPJygGxarwkxWE6zsEpOXaSJjdKfe1qIDYrLiQoqGNhdFceoWp1DP0r4xA5w4y6iVBzCnOJdjSRODZkhhnFE62KPUQNkQs3wwl0anEohEPRGB6aHk407sLzvR9ADitgJRNfx1qhiVVo4AIeuR7WPQYtCFTqsCCTrri4Z3kYiSnoDHuoS9Tg2tA7eLGWwiUi/i4CCUaxkwJPAoZx3Ydt+UgwtywkJGmdM/A/ygE2fR8qFTEclZGXYzhZDOPtTQk5QvFqVMB2AfhV97BY9iA7TtBIhUl6VJNnC7pjzOli/Bw4DsRDiBAR76siPEJQZByNFEgQ4I7l4/ctF7bjYlCyvLO14jx9pSE69ThtzWULpaFbHsc/FY6IKOA5VcD+mIwECG7rPmwO3Cq7KJZMJO1NvJks50a0+vv06sSl/HstbaNRttH3bT4iF+UULELxc1zDeMFB4AdwPB/gAXzHQrWxhjfcheDI9ubvovUts/TY4EFoybrzQv5CV6pw7dRVg5P7WicKQhdsOQ5qVaAW1xD3NtFsZtBfWMaRjp6fmvf2fpVbnHk6Uy69iFA0llq4/ssnS/cmjv+lM62gF5GpakKLZ2CbkUVSZlBl1Wpt3/fj4AuvfawX1rN7hg49DQCArlegKBFlefbOyOP5u8fTywvPFirGTurZSETD2URq22RtQ+uF7uHDY3bFMGoaWgEA/wJXTqwGMnRz3wAAACZ6VFh0RGVzY3JpcHRpb24AAHjacy5KTSxJTVEozyzJUHD39A0AADedBeKasdl2AAAAWXpUWHRTb2Z0d2FyZQAAeNrzzE1MT/VNTM9MzlYw0zPSs1AwMNU3MNc3NFIINDRTSMvMSbXSLy0u0i/OSCxK1fdEKNc10zPSs9BPyU/Wz8xLSa3QyyjJzQEArU4YrHQVLmsAAAAhelRYdFRodW1iOjpEb2N1bWVudDo6UGFnZXMAAHjaMwQAADIAMgwS4oQAAAAhelRYdFRodW1iOjpJbWFnZTo6aGVpZ2h0AAB42jM3NwIAAUgAoaqeVecAAAAgelRYdFRodW1iOjpJbWFnZTo6V2lkdGgAAHjaM7e0BAABUwCqnbZQAwAAACJ6VFh0VGh1bWI6Ok1pbWV0eXBlAAB42svMTUxP1U/PTAMAEVUDaQ/K+N8AAAAgelRYdFRodW1iOjpNVGltZQAAeNozNDExNTI1NLM0AgALGgIIywTFYAAAAB56VFh0VGh1bWI6OlNpemUAAHjaM9IzTNU2MMpOAgAKVQJRIy6XwwAAABx6VFh0VGh1bWI6OlVSSQAAeNpLy8xJtdLX1wcADJoCaJRAUaoAAAAASUVORK5CYII=\">" +
      "<link rel=\"stylesheet\" type=\"text/css\" href=\"//maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css\"/>" +
      "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js\"></script>" +
      "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js\"></script>" +
      "</head>" +
      "<body class=\"container\">" + htmlHeader +
      s"<h3>Logical Partitions <small>ACTIVE ${activePartitions.size} / ${numLogicalPartitions}</small></h3>" +
      htmlMetricsTable(activePartitions) +
      "</body></html>"
  }

  private def htmlHeader: String = {
    val p = math.round((100 * getProgress))
    val style = if (p >= 99) "info" else if (p > 90) "success" else if (p > 30) "warning" else "danger"

    def htmlMemory: String = {
      if (metrics.containsKey(Metrics.CONTAINER_MEMORY)) {
        val containersMemory = metrics.get(Metrics.CONTAINER_MEMORY).value.toInt
        val usedMemory = containersMemory + masterMemoryMb
        val totalCores = metrics.get(Metrics.CONTAINER_CORES).value.toInt + masterCores
        val maxMemory = totalMainMemoryMb + taskOverheadMemMb * numLogicalPartitions + masterMemoryMb
        val memoryDetails = s"total memory utilised (${containersMemory / 1024} + ${masterMemoryMb / 1024} Gb) of total allocated ${maxMemory / 1024} Gb"
        s"<a title='${memoryDetails}'>MEMORY: <b>${usedMemory / 1024} Gb</b></a>, <a>TOTAL CORES: <b>${totalCores}</b></a>, "
      } else {
        ""
      }
    }

    return s"<div class='row'>" +
      s"<h2>${appClass} <small>${htmlMemory}<a title='KAFKA BROKERS: ${kafkaBrokers}'>KAFKA GROUP ID: <b>${kafkaGroupId}</b></a></small></h2></div>" +
      s"<div class='row'><div class='progress'>" +
      s"<div class='progress-bar progress-bar-${style} progress-bar-striped' role='progressbar' aria-valuenow='${p}' aria-valuemin='0' aria-valuemax='100' style='width:${p}%'>" +
      s"${p}% <i>input streams: </i> ${topics.mkString(",&nbsp;")}" +
      s"</div></div></div>"
  }

  private def htmlMetricsTable(activePartitions: Seq[Int]): String = {

    val metrics = this.metrics.asScala
    def getTab(label: String): String = label.split(":") match {
      case y if (y.length > 1) => y(0)
      case _ => "_"
    }
    val tabs = metrics.filter(_._1.contains(":")).map(m => getTab(m._1)).toSet + "_"

    def htmlTabContent(tab: String) = {
      val filtered = metrics.filter(m => getTab(m._1) == tab).toSeq.sortWith((a,b) => a._1 < b._1)
      def htmlPartitionTR(partition: Int) = {
        s"<tr><td>${partition}</td>" +
          filtered.map { case (name, metric) => s"<td>${htmlMetric(partition, metric)}</td>" }.mkString
      }
      s"<table class='table table-striped table-condensed'>" +
        s"<thead>" +
        s"<tr><th>∑</th>" + filtered.map { case (name, metric) => s"<th>${htmlMetric(-1, metric)}</th>" }.mkString + "</tr>" +
        s"<tr><td><i>P</i></td>${filtered.map(x => s"<td>${x._1.replace(":", " ")}</td>").mkString}</tr>" +
        s"</thead>" +
        s"<tbody>${activePartitions.map(htmlPartitionTR).mkString("\n")}</tbody></table>"
    }

    return if (tabs.size <= 1) {
      htmlTabContent("_")
    } else {
      "<ul class=\"nav nav-tabs\">" +
        (tabs.map(tab => s"<li class='${if (tab == "_") "active" else ""}'><a data-toggle='tab' href='#${tab.replace(" ", "_")}'>${tab}</a></li>")).mkString("\n") +
        "</ul><div class=\"tab-content\">" +
        (tabs.map(tab => s"<div id='${tab.replace(" ", "_")}' class='tab-pane fade in ${if (tab == "_") "active" else ""}'>${htmlTabContent(tab)}</div>")).mkString("\n") +
        "</div>"
    }
  }


  private def htmlMetric(partition: Int, metric: Metric) = {
    val value = if (partition == -1) metric.value else if (metric.value(partition) != null) metric.value(partition) else null
    val hint = if (partition == -1) "" else metric.hint(partition)
    val style = (if (partition == -1) " progress-bar-striped"
    else if (hint.contains("bootstrap")) " progress-bar-danger" else "")
    val content = metric match {
      case m: Progress => {
        val p = math.round(if (value == null) 0 else (value.toFloat * 100))
        s"<div class='progress' style='margin-bottom: 0 !important;'>" +
          s"<div class='progress-bar${style}' role='progressbar' aria-valuenow='${p}' aria-valuemin='0' aria-valuemax='100' style='width:${p}%'>${p}%</div></div>"
      }
      case m => s"${if (value != null) value else "N/A"}"
    }
    if (partition == -1) content
    else metric.hint(partition) match {
      case "" => content
      case hint => s"<a title='${hint}'>${content}</a>"
    }
  }


//TODO show completed containers with links
//    private def getCompletedContainers(partition: Int): List[(String, String, String, Int)] = {
//      val builder = List.newBuilder[(String, String, String, Int)]
//      val it = app.getCompletedContainers.entrySet.iterator
//      while (it.hasNext) {
//        val entry = it.next
//        val container = entry.getValue
//        val containerLaunchArgLogicalPartition = container.args(2).toInt
//        if (containerLaunchArgLogicalPartition == partition) {
//          builder += ((
//            container.getNodeAddress + "/" + entry.getKey,
//            container.getLogsUrl("stderr"),
//            container.getLogsUrl("stdout"),
//            container.getStatus.getExitStatus
//            ))
//        }
//      }
//      builder.result
//    }

}


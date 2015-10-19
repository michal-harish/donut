package org.apache.donut.ui

import java.util.concurrent.ConcurrentSkipListMap

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.Response.Status
import fi.iki.elonen.NanoHTTPD.{IHTTPSession, Response}
import org.apache.donut.DonutApp
import org.apache.donut.metrics.{Info, Metric, Progress}

import scala.collection.JavaConverters._

/**
 * Created by mharis on 17/10/15.
 */
private[ui] class WebUIServer(val app: DonutApp[_], val host: String, val port: Int) extends NanoHTTPD(port) {

  private val metrics = new ConcurrentSkipListMap[String, Metric]()

  val appClass = app.getClass.getSimpleName
  val taskType = app.taskClass.getSimpleName
  val subsType = (if (app.config.getProperty("cogroup", "false").toBoolean) "(cogroup)" else "") +
    (if (app.config.getProperty("max.tasks", "-1") == "-1") "" else s"(max.tasks)")

  def getProgress(partition: Int = -1): Float = {
    metrics.get("input progress") match {
      case metric: Progress if (metric != null) => partition match {
        case -1 => metric.value.toFloat
        case p => metric.value(p).toFloat
      }
      case _ => 0f
    }
  }

  override def serve(session: IHTTPSession): Response = {
    session.getMethod match {
      case NanoHTTPD.Method.POST => session.getUri match {
        case "/metrics" => handlePostMetrics(session)
        case "/errors" => handlePostErrors(session)
      }
      case NanoHTTPD.Method.GET => session.getUri match {
        case "" | "/" => serveMain(session)
        case uri => new Response(Status.NOT_FOUND, NanoHTTPD.MIME_HTML, s"<em>Not Found<em><pre>${uri}</pre>")
      }
      case m => new Response(Status.METHOD_NOT_ALLOWED, NanoHTTPD.MIME_HTML, s"<em>Method Not Allowed<em><pre>${m}</pre>")
    }
  }

  def handlePostErrors(session: IHTTPSession): Response = {
    try {
      val params = session.getParms
      val partition = params.remove("p").toInt
      val message: String = params.get("e")
      val trace: String = params.get("t")
      if (!metrics.containsKey("last-error")) metrics.put("last-error", new Info)
      val metric: Metric = metrics.get("last-error")
      metric.put(partition, metric.value(partition) + s"${message}<br/><pre>\n${trace}</pre>", "")
      new Response(Status.ACCEPTED, NanoHTTPD.MIME_PLAINTEXT, "")
    } catch {
      case e: IllegalArgumentException => new Response(Status.BAD_REQUEST, NanoHTTPD.MIME_PLAINTEXT, e.getMessage)
      case i: Throwable => {
        i.printStackTrace()
        new Response(Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, i.getStackTraceString)
      }
    }
  }


  private def handlePostMetrics(session: IHTTPSession): Response = {
    try {
      val params = session.getParms
      val partition = params.remove("p").toInt
      val name: String = params.get("n")
      val value: String = params.get("v")
      val hint: String = params.get("h")
      val cls = Class.forName(params.get("c")).asSubclass(classOf[Metric])
      if (!metrics.containsKey(name)) metrics.put(name, cls.newInstance)
      val metric: Metric = metrics.get(name)
      if (!cls.isInstance(metric)) {
        throw new IllegalArgumentException(s"Metric ${name} expected to be of type ${metric.getClass}")
      }
      //log.debug(s"METRIC ${name}, ${value}, ${hint}")
      metric.put(partition, value, hint)
      new Response(Status.ACCEPTED, NanoHTTPD.MIME_PLAINTEXT, "")
    } catch {
      case e: IllegalArgumentException => new Response(Status.BAD_REQUEST, NanoHTTPD.MIME_PLAINTEXT, e.getMessage)
      case i: Throwable => {
        i.printStackTrace()
        new Response(Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, i.getStackTraceString)
      }
    }
  }

  private def serveMain(session: IHTTPSession): Response = {
    val activePartitions = (0 to app.numLogicalPartitions - 1).filter(p => metrics.asScala.exists(_._2.value(p) != null))
    new Response(
      "<!DOCTYPE html><html lang=\"en\">" +
        "<head>" +
        "<meta charset=\"utf-8\" />" +
        "<link rel=\"icon\" href=\"data:image/ico;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABmJLR0QAAAAAAAD5Q7t/AAAACXBIWXMAAABIAAAASABGyWs+AAAACXZwQWcAAAAQAAAAEABcxq3DAAADjklEQVQ4yzWTTWwUZQCG32/mm9md3dmdbncLpfQH2mJp+v9DGgQbYgVDYjTqATVEDeIJEg4eJMaDMR6M8eTFiEkTExL1wMGgFCMpCtIKKUKLpb+EZel2222X7c7szv98nwf0uT/P7SH4j5nVPE4VU3g3Uqm56fgDs77QUyORjnUPaZPxqRQhUy/7T1anEcJo347/NRAAOLu0hV0Spxct4fW1gJyWOe+1iRDRBCIA4Cs+t0zGlyjn53tC/PuLy2J2rMfF0aYkyBcPt9BIiXpODz40Qc8MyEKsTSIw/QAuJ1BFAhAg53Ncd8DWfDbdRvlHlztOXP5y/huQ0fnp8A/ejs9mA3rmsELpgCzgSsnFVKkCIxChUqBDlXEoHgIVRFwyGR44fmYPZW/dNpwbYv3pT/dNuPh8tyTFuiSCG5aPKxs2Srk8nPUsLKOMBy6gy2HsjxAMhBh8P9BWKmbLsTj/jU7q/rDGWHW/ZKGOS5gzbUgewDcyeCb9J4LAQWbvAdTVdKPJygGxarwkxWE6zsEpOXaSJjdKfe1qIDYrLiQoqGNhdFceoWp1DP0r4xA5w4y6iVBzCnOJdjSRODZkhhnFE62KPUQNkQs3wwl0anEohEPRGB6aHk407sLzvR9ADitgJRNfx1qhiVVo4AIeuR7WPQYtCFTqsCCTrri4Z3kYiSnoDHuoS9Tg2tA7eLGWwiUi/i4CCUaxkwJPAoZx3Ydt+UgwtywkJGmdM/A/ygE2fR8qFTEclZGXYzhZDOPtTQk5QvFqVMB2AfhV97BY9iA7TtBIhUl6VJNnC7pjzOli/Bw4DsRDiBAR76siPEJQZByNFEgQ4I7l4/ctF7bjYlCyvLO14jx9pSE69ThtzWULpaFbHsc/FY6IKOA5VcD+mIwECG7rPmwO3Cq7KJZMJO1NvJks50a0+vv06sSl/HstbaNRttH3bT4iF+UULELxc1zDeMFB4AdwPB/gAXzHQrWxhjfcheDI9ubvovUts/TY4EFoybrzQv5CV6pw7dRVg5P7WicKQhdsOQ5qVaAW1xD3NtFsZtBfWMaRjp6fmvf2fpVbnHk6Uy69iFA0llq4/ssnS/cmjv+lM62gF5GpakKLZ2CbkUVSZlBl1Wpt3/fj4AuvfawX1rN7hg49DQCArlegKBFlefbOyOP5u8fTywvPFirGTurZSETD2URq22RtQ+uF7uHDY3bFMGoaWgEA/wJXTqwGMnRz3wAAACZ6VFh0RGVzY3JpcHRpb24AAHjacy5KTSxJTVEozyzJUHD39A0AADedBeKasdl2AAAAWXpUWHRTb2Z0d2FyZQAAeNrzzE1MT/VNTM9MzlYw0zPSs1AwMNU3MNc3NFIINDRTSMvMSbXSLy0u0i/OSCxK1fdEKNc10zPSs9BPyU/Wz8xLSa3QyyjJzQEArU4YrHQVLmsAAAAhelRYdFRodW1iOjpEb2N1bWVudDo6UGFnZXMAAHjaMwQAADIAMgwS4oQAAAAhelRYdFRodW1iOjpJbWFnZTo6aGVpZ2h0AAB42jM3NwIAAUgAoaqeVecAAAAgelRYdFRodW1iOjpJbWFnZTo6V2lkdGgAAHjaM7e0BAABUwCqnbZQAwAAACJ6VFh0VGh1bWI6Ok1pbWV0eXBlAAB42svMTUxP1U/PTAMAEVUDaQ/K+N8AAAAgelRYdFRodW1iOjpNVGltZQAAeNozNDExNTI1NLM0AgALGgIIywTFYAAAAB56VFh0VGh1bWI6OlNpemUAAHjaM9IzTNU2MMpOAgAKVQJRIy6XwwAAABx6VFh0VGh1bWI6OlVSSQAAeNpLy8xJtdLX1wcADJoCaJRAUaoAAAAASUVORK5CYII=\">" +
        "<link rel=\"stylesheet\" type=\"text/css\" href=\"//maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css\"/>" +
        "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js\"></script>" +
        "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js\"></script>" +
        "</head>" +
        "<body class=\"container\">" + htmlHeader +
        s"<h3>Logical Partitions <small>ACTIVE ${activePartitions.size} / ${app.numLogicalPartitions}</small></h3>" +
        htmlMetricsTable(activePartitions) +
        "</body></html>"
    )
  }

  private def htmlHeader: String = {
    val p = math.round((100 * getProgress(-1)))
    val style = if (p >= 99) "info" else if (p > 90) "success" else if (p > 30) "warning" else "danger"

    def htmlMemory: String = {
      val c = app.getRunningContainers.asScala.map { case (cid, container) => container.capability }
      val containersMemory = c.map(_.getMemory).sum
      val usedMemory = containersMemory + app.masterMemoryMb
      val totalCores = c.map(_.getVirtualCores).sum + app.masterCores
      val maxMemory = app.totalMainMemoryMb + app.taskOverheadMemMb * app.numLogicalPartitions + app.masterMemoryMb
      val memoryDetails = s"${c.size} containers and one master using (${containersMemory / 1024} + ${app.masterMemoryMb / 1024} Gb) of total available ${maxMemory / 1024} Gb"
      s"<a title='${memoryDetails}'>MEMORY: <b>${usedMemory / 1024} Gb</b></a>, <a>TOTAL CORES: <b>${totalCores}</b></a>"
    }

    return s"<div class='row'>" +
      s"<h2>${appClass} <small>${htmlMemory}, <a title='KAFKA BROKERS: ${app.kafkaUtils.kafkaBrokers}'>KAFKA GROUP ID: ${app.config.get("group.id")}</a></small></h2></div>" +
      s"<div class='row'><div class='progress'>" +
      s"<div class='progress-bar progress-bar-${style} progress-bar-striped' role='progressbar' aria-valuenow='${p}' aria-valuemin='0' aria-valuemax='100' style='width:${p}%'>" +
      s"${p}% <i>input streams ${subsType}: </i> ${app.topics.mkString("&nbsp;&amp;&nbsp;")}" +
      s"</div></div></div>"
  }

  private def htmlMetricsTable(activePartitions: Seq[Int]): String = {

    val metrics = this.metrics.asScala
    val tabs = metrics.filter(_._1.contains(":")).map(_._1.split(":")(0)).toSet

    def htmlTabContent(tab: String) = {
      val filtered = if (tabs.size == 0) metrics else metrics.filter(m => !m._1.contains(":") || m._1.split(":")(0) == tab)
      def htmlPartitionTR(partition: Int) = {
        s"<tr><td>${partition}</td>" +
          filtered.map { case (name, metric) => s"<td>${htmlMetric(partition, metric)}</td>" }.mkString +
          "<td>" + htmlContainers(partition) + "</td></tr>"
      }
      s"<table class='table table-striped table-condensed'>" +
        s"<thead>" +
        s"<tr><th>∑</th>" + filtered.map { case (name, metric) => s"<th>${htmlMetric(-1, metric)}</th>" }.mkString +
        "<th>-</th></tr>" +
        s"<tr><td><i>P</i></td>${filtered.map(x => s"<td>${x._1}</td>").mkString}<td>container</td></tr>" +
        s"</thead>" +
        s"<tbody>${activePartitions.map(htmlPartitionTR).mkString("\n")}</tbody></table>"
    }

    return if (tabs.size == 0) {
      htmlTabContent("*")
    } else {
      "<ul class=\"nav nav-tabs\">" +
        (tabs.map(tab => s"<li><a data-toggle='tab' href='#${tab.replace(" ", "_")}'>${tab}</a></li>")).mkString("\n") +
        "</ul><div class=\"tab-content\">" +
        (tabs.map(tab => s"<div id='${tab.replace(" ", "_")}' class='tab-pane fade in active'><h3>${tab}</h3>${htmlTabContent(tab)}</div>")).mkString("\n") +
        "</div>"
    }
  }


  private def htmlContainers(partition: Int): String = {

    def htmlActiveContainer(partition: Int): String = {
      val (address, stderrLogs, stdoutLogs) = getActiveContainer(partition)
      (if (address == null) "" else s"<a title='${address}'>RUNNING</a>&nbsp;<a href='${stderrLogs}'>stderr</a>&nbsp;<a href='${stdoutLogs}'>stdout</a>")

    }

    def htmlCompletedContainers(partition: Int): String = {
      getCompletedContainers(partition).map {
        case (address, stderrLogs, stdoutLogs, exitStatus) => {
          s"<a title='${address}'>EXITED(${exitStatus})</a>&nbsp;<a href='${stderrLogs}'>stderr</a>&nbsp;<a href='${stdoutLogs}'>stdout</a>"
        }
      }.mkString("<br/>")
    }

    return s"<div>${htmlActiveContainer(partition)}<br/>" + htmlCompletedContainers(partition) + "</div>"

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

  /**
   *
   * @param partition
   * @return (node/container, stderrLogsUrl, stdoutLogsUrl)
   */
  private def getActiveContainer(partition: Int): (String, String, String) = {
    val it = app.getRunningContainers.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next
      val container = entry.getValue
      val containerLaunchArgLogicalPartition = container.args(2).toInt
      if (containerLaunchArgLogicalPartition == partition) {
        val id = container.getNodeAddress() + "/" + entry.getKey.toString
        return (id, container.getLogsUrl("stderr"), container.getLogsUrl("stdout"))
      }
    }
    (null, null, null)
  }

  private def getCompletedContainers(partition: Int): List[(String, String, String, Int)] = {
    val builder = List.newBuilder[(String, String, String, Int)]
    val it = app.getCompletedContainers.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next
      val container = entry.getValue
      val containerLaunchArgLogicalPartition = container.args(2).toInt
      if (containerLaunchArgLogicalPartition == partition) {
        builder += ((
          container.getNodeAddress + "/" + entry.getKey,
          container.getLogsUrl("stderr"),
          container.getLogsUrl("stdout"),
          container.getStatus.getExitStatus
          ))
      }
    }
    builder.result
  }

}


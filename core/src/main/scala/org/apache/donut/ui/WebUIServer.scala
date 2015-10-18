package org.apache.donut.ui

import java.util.concurrent.ConcurrentSkipListMap

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.Response.Status
import fi.iki.elonen.NanoHTTPD.{IHTTPSession, Response}
import org.apache.donut.DonutApp
import org.apache.donut.metrics.{Info, Metric, Progress}
import org.apache.hadoop.yarn.api.records.ContainerId

import scala.collection.JavaConverters._

/**
 * Created by mharis on 17/10/15.
 */
private[ui] class WebUIServer(val app: DonutApp[_], val host: String, val port: Int) extends NanoHTTPD(port) {

  private val metrics = new ConcurrentSkipListMap[String, Metric]()

  val appClass = app.getClass.getSimpleName
  val taskType = app.taskClass.getSimpleName
  val subsType = (if (app.config.getProperty("cogroup", "false").toBoolean) "cogroup " else "") +
    (if (app.config.getProperty("max.tasks", "-1") == "-1") "" else s"bounded ")

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
        "<meta charset=\"utf-8\" /> " +
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
      val c = app.getRunningContainers.asScala.map{ case (cid, container) => container.capability}
      val totalMb = c.map(_.getMemory).sum
      val totalCores = c.map(_.getVirtualCores).sum
      s"<p>Total Memory: <em>${totalMb} Mb</em>, Total Cores: <em>${totalCores}</em></p>"
    }

    return s"<div class='row'>" +
      s"<h2>${appClass} <small>${htmlMemory}| KAFKA GROUP ID: ${app.config.get("group.id")}</small></h2></div>" +
      s"<div class='row'><div class='progress'>" +
      s"<div class='progress-bar progress-bar-${style} progress-bar-striped' role='progressbar' aria-valuenow='${p}' aria-valuemin='0' aria-valuemax='100' style='width:${p}%'>" +
      s"${p}% <i>input streams(${subsType}): </i> ${app.topics.mkString("&nbsp;&amp;&nbsp;")}" +
      s"</div></div></div>"

  }


  private def htmlMetricsTable(activePartitions: Seq[Int]): String = {

    val metrics = this.metrics.asScala
    val tabs = metrics.filter(_._1.contains(":")).map(_._1.split(":")(0)).toSet

    def htmlTabContent(tab: String ) = {
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
        (tabs.map(tab => s"<li><a data-toggle='tab' href='#${tab.replace(" ","_")}'>${tab}</a></li>")).mkString("\n") +
        "</ul><div class=\"tab-content\">" +
        (tabs.map(tab => s"<div id='${tab.replace(" ","_")}' class='tab-pane fade in active'><h3>${tab}</h3>${htmlTabContent(tab)}</div>")).mkString("\n") +
        "</div>"
    }
  }


  private def htmlContainers(partition: Int): String = {

    def htmlActiveContainer(partition: Int): String = {
      val (containerId, stderrLogs, stdoutLogs) = getActiveContainer(partition)
      (if (containerId == null) "" else s"${containerId}:&nbsp;<a href='${stderrLogs}'>stderr</a>&nbsp;<a href='${stdoutLogs}'>stdout</a>")

    }

    def htmlCompletedContainers(partition: Int): String = {
      getCompletedContainers(partition).map {
        case (affress, stderrLogs, stdoutLogs, existStatus) => {
          s"${affress} (exit status ${existStatus}):&nbsp;<a href='${stderrLogs}'>stderr</a>&nbsp;<a href='${stdoutLogs}'>stdout</a>"
        }
      }.mkString("<br/>")
    }

    return "<div>[<em>" + htmlActiveContainer(partition) + "] </em><br/>" + htmlCompletedContainers(partition) + "</div>"

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
        return (id,  container.getLogsUrl("stderr"), container.getLogsUrl("stdout"))
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


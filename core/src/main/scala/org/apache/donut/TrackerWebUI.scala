package org.apache.donut

import java.net.URL

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.Response.Status
import fi.iki.elonen.NanoHTTPD.{IHTTPSession, Response}
import org.apache.donut.metrics.Metric
import org.apache.hadoop.yarn.api.records.ContainerId

import scala.collection.mutable


/**
 * Created by mharis on 14/10/15.
 */

class TrackerWebUI(val app: DonutApp[_], val host: String, val port: Int) extends NanoHTTPD(port) {

  private val metrics = mutable.LinkedHashMap[String, Metric]()

  val appClass = app.getClass.getSimpleName
  val taskType = app.taskClass.getSimpleName
  val subsType = (if (app.config.getProperty("cogroup", "false").toBoolean) "cogroup " else "") +
    (if (app.config.getProperty("max.tasks","-1") == "-1") "" else s"bounded ")

  def url = new URL("http", host, getListeningPort, "/")

  override def serve(session: IHTTPSession): Response = session.getMethod match {
    case NanoHTTPD.Method.POST => session.getUri match {
      case "/metrics" => handlePostMetrics(session)
    }
    case NanoHTTPD.Method.GET => session.getUri match {
      case "" | "/" => serveMain(session)
      case uri => new Response(Status.NOT_FOUND, NanoHTTPD.MIME_HTML, s"<em>Not Found<em><pre>${uri}</pre>")
    }
    case m => new Response(Status.METHOD_NOT_ALLOWED, NanoHTTPD.MIME_HTML, s"<em>Method Not Allowed<em><pre>${m}</pre>")
  }

  private def handlePostMetrics(session: IHTTPSession): Response = {
    try {
      val params = session.getParms
      val partition = params.remove("p").toInt
      val name: String = params.get("n")
      val value: String = params.get("v")
      val cls = Class.forName(params.get("c")).asSubclass(classOf[Metric])
      if (!metrics.contains(name)) metrics.put(name, cls.newInstance)
      val metric: Metric = metrics(name)
      if (!cls.isInstance(metric)) {
        throw new IllegalArgumentException(s"Metric ${name} expected to be of type ${metric.getClass}")
      }
      metric.put(partition, value)
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
    new Response(
      "<!DOCTYPE html><html lang=\"en\">" +
        "<head><link rel=\"stylesheet\" type=\"text/css\" href=\"//maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css\"/></head>" +
        "<body class=\"container\">" + htmlHeader +
        s"<h3>Logical Partitions <small>ACTIVE ${app.getRunningContainers.size} / ${app.numLogicalPartitions}</small></h3>" + htmlMetricsTable +
        s"<h3>Input Topics <small>${subsType}</small></h3>" +
        s"<ul>${app.topics.map(t => s"<li>${t}</li>").mkString}</ul>" +
        s"<h3>Progress <small>${100.0 * app.getLastProgress} %</small></h3>" +
        s"<ul>${app.getLastOffsets.map(p => s"<li>${p._1}/${p._2} ${100 * (p._4 - p._3) / (p._5 - p._3)}%</li>").mkString}</ul>" +
        "</body></html>"
    )
  }

  private def htmlHeader: String = {
    val p = (100 * app.getLastProgress).toInt
    val style = if (p > 90) "success" else if (p > 50) "info" else if (p > 20) "warning" else "danger"
    s"<div class='row'><h2>${appClass} <small>&nbsp;YARN Application Master</small></h2></div>" +
    s"<div class='row'><div class='progress'>" +
      s"<div class='progress-bar progress-bar-${style}' role='progressbar' aria-valuenow='${p}' aria-valuemin='0' aria-valuemax='100' style='width:${p}%'>${p}%</div>" +
    s"</div></div>"
  }

  private def htmlMetricsTable: String = {
    s"<table class='table table-striped table-condensed'>" +
      s"<thead>" +
      s"<tr><th>*</th>${metrics.map(x => s"<th>${x._2.get}</th>").mkString}<th>-</th><th>-</th><th>-</th></tr>" +
      s"<tr><td>part.</td>${metrics.map(x => s"<td>${x._1}</td>").mkString}<td>stderr</td><td>stdout</td><td>container_id</td></tr>" +
      s"</thead>" +
      s"<tbody>${(0 to app.numLogicalPartitions -1).map(htmlPartitionRow).mkString("\n")}</tbody></table>"
  }

  private def htmlPartitionRow(partition:Int): String = {
    val (containerId, stderrLogs, stdoutLogs) = getContainerForPartition(partition)
    s"<tr><td>${partition}</td>" + metrics.map { case (name, metric) => s"<td>${if (metric.get(partition) != null) metric.get(partition) else "N/A"}</td>" }.mkString +
      (if (containerId != null) s"<td><a href='${stderrLogs}'>stderr</a></td><td><a href='${stdoutLogs}'>stdout</a></td><td>${containerId}</td>"
      else "<td></td><td></td><td></td>") +
      s"</tr>"
  }

  /**
   *
   * @param partition
   * @return (containerId, stderrLogsUrl, stdoutLogsUrl)
   */
  private def getContainerForPartition(partition: Int): (ContainerId, String, String) = {
    val it = app.getRunningContainers.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next
      val containerSpec = entry.getValue
      val containerLaunchArgLogicalPartition = containerSpec.args(2).toInt
      if (containerLaunchArgLogicalPartition == partition) {
        return (entry.getKey, containerSpec.getLogsUrl("stderr"), containerSpec.getLogsUrl("stdout"))
      }
    }
    (null, null, null)
  }

}


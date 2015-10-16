package org.apache.donut

import java.net.URL

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.Response.Status
import fi.iki.elonen.NanoHTTPD.{IHTTPSession, Response}
import org.apache.hadoop.yarn.api.records.ContainerId

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * Created by mharis on 14/10/15.
 */

class TrackerWebUI(val app: DonutApp[_], val host: String, val port: Int) extends NanoHTTPD(port) {

  class TaskInfo {
    var message: String = null
    var logs: URL = null
  }

  private val vars = mutable.HashSet[String]()
  private val tasks = new java.util.TreeMap[Int, mutable.Map[String, String]]()

  val appClass = app.getClass.getSimpleName
  val taskType = app.taskClass.getSimpleName
  val subsType = (if (app.config.getProperty("cogroup", "false").toBoolean) "cogroup " else "") +
    (if (app.config.getProperty("max.tasks","-1") == "-1") "" else s"bounded ")

  def url = new URL("http", host, getListeningPort, "/")

  override def serve(session: IHTTPSession): Response = session.getMethod match {
    case NanoHTTPD.Method.POST => handlePost(session)
    case NanoHTTPD.Method.GET => session.getUri match {
      case "" | "/" => serveMain(session)
      case uri => new Response(Status.NOT_FOUND, NanoHTTPD.MIME_HTML, s"<em>Not Found<em><pre>${uri}</pre>")
    }
    case m => new Response(Status.METHOD_NOT_ALLOWED, NanoHTTPD.MIME_HTML, s"<em>Method Not Allowed<em><pre>${m}</pre>")
  }

  private def handlePost(session: IHTTPSession): Response = {
    try {
      val params = session.getParms
      val partition = params.remove("p").toInt
      if (!tasks.containsKey(partition)) tasks.put(partition, mutable.HashMap())
      params.asScala.foreach(param => {
        vars.add(param._1)
        tasks.get(partition) += param
      })
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
    val containers = getContainers
    new Response(
      "<!DOCTYPE html><html lang=\"en\">" +
        "<head><link rel=\"stylesheet\" type=\"text/css\" href=\"//maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css\"/></head>" +
        "<body class=\"container\">" +
        s"<p><h2>${appClass} <small>&nbsp;YARN Application Master</small></h2><hr/></p>" +
        s"<h3>Logical Partitions <small>ACTIVE ${tasks.size} / ${app.numLogicalPartitions}</small></h3>" +
        s"<table class='table table-striped table-condensed'><tbody>${
          tasks.asScala.map { case (p, info) => {
            s"<tr><td>${p}</td>" + vars.map(i => s"<td>${info(i)}</td>").mkString +
              (if (containers.contains(p)) s"<td><a href='${containers(p)._2}'>stderr</a></td><td><a href='${containers(p)._3}'>stdout</a></td><td>${containers(p)._1}</td>"
              else "<td></td><td></td><td></td>") +
              s"</tr>"
          }
          }.mkString
        }</tbody></table>" +
        s"<h3>Input Topics <small>${subsType}</small></h3>" +
        s"<ul>${app.topics.map(t => s"<li>${t}</li>").mkString}</ul>" +
        s"<h3>Progress <small>${100.0 * app.getLastProgress} %</small></h3>" +
        s"<ul>${app.getLastOffsets.map(p => s"<li>${p._1}/${p._2} ${100 * (p._4 - p._3) / (p._5 - p._3)}%</li>").mkString}</ul>" +
        "</body></html>"
    )
  }

  private def getContainers: Map[Int, (ContainerId, String, String)] = {
    app.getRunningContainers.asScala.map { case (id, spec) => {
      (spec.args(2).toInt ->(id, spec.getLogsUrl("stderr"), spec.getLogsUrl("stdout")))
    }
    }.toMap
  }
}


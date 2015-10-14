package org.apache.donut

import java.net.{NetworkInterface, URL}

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.{Response, IHTTPSession, Method}

import scala.collection.JavaConverters._


/**
 * Created by mharis on 14/10/15.
 */

class WebUI(val container_id: String) extends NanoHTTPD(0) {
  private val interfaces = NetworkInterface.getNetworkInterfaces.asScala
  private val address = interfaces.flatMap(_.getInterfaceAddresses().asScala).map(_.getAddress)
    .filter(a => a.isSiteLocalAddress).next

  def url = new URL("http", address.getCanonicalHostName, getListeningPort, s"/${container_id}")

  override def serve(session: IHTTPSession): Response = {
    val method = session.getMethod
    val uri = session.getUri
    println(s"${method} ${uri}")
    var msg = "<html><body><h1>Hello server</h1>\n"
    val parms = session.getParms()
    if (parms.get("username") == null)
      msg +=
        "<form action='?' method='get'>\n" +
          "  <p>Your name: <input type='text' name='username'></p>\n" +
          "</form>\n";
    else
      msg += "<p>Hello, " + parms.get("username") + "!</p>";

    msg += "</body></html>\n";

    new NanoHTTPD.Response(msg)
  }
}


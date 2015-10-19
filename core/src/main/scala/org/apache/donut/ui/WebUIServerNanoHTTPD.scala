//package org.apache.donut.ui
//
//
//import fi.iki.elonen.NanoHTTPD
//import fi.iki.elonen.NanoHTTPD.Response.Status
//import fi.iki.elonen.NanoHTTPD.{IHTTPSession, Response}
//import scala.collection.JavaConverters._
//
///**
// * Created by mharis on 19/10/15.
// */
//class WebUIServerNanoHTTPD(host: String, port: Int) extends WebUIServer(host, port) {
//
//
//  val httpd = new NanoHTTPD(port) {
//    override def serve(session: IHTTPSession): Response = {
//      val (status, mime, body) = handle(session.getMethod.toString, session.getUri, session.getParms.asScala.toMap)
//      status match {
//        case 200 => new Response(Status.OK, mime, body)
//        case 202 => new Response(Status.ACCEPTED, mime, body)
//        case 400 => new Response(Status.BAD_REQUEST, mime, body)
//        case 404 => new Response(Status.NOT_FOUND, mime, body)
//        case _ => new Response(Status.INTERNAL_ERROR, mime, body)
//      }
//    }
//  }
//
//  override def start: Unit = httpd.start
//
//  override def getListeningPort: Int = httpd.getListeningPort
//
//  override def stop: Unit = httpd.stop
//}
//

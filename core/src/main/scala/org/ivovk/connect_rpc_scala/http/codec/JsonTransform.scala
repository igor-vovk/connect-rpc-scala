package org.ivovk.connect_rpc_scala.http.codec

import org.json4s.JsonAST.{JObject, JValue}

sealed trait JsonTransform {
  def apply(body: JValue): JValue
}

case object AsIsJsonTransform extends JsonTransform {
  override def apply(body: JValue): JValue = body
}

case class SubKeyJsonTransform(key: String) extends JsonTransform {
  override def apply(body: JValue): JValue = JObject(key -> body)
}

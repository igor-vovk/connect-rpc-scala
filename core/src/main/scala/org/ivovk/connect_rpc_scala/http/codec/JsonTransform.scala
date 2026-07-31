package org.ivovk.connect_rpc_scala.http.codec

import io.circe.Json

sealed trait JsonTransform extends (Json => Json)

case object AsIsJsonTransform extends JsonTransform {
  override def apply(body: Json): Json = body
}

case class SubKeyJsonTransform(key: String) extends JsonTransform {
  override def apply(body: Json): Json =
    key.split('.').reverse.foldLeft(body) { (json, field) =>
      Json.obj(field -> json)
    }
}

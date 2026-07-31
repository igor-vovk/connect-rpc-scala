package org.ivovk.connect_rpc_scala.http.json

import com.google.protobuf.UnsafeByteOperations.unsafeWrap
import connectrpc.ErrorDetailsAny
import io.circe.Json
import scalapb_json.JsonFormatException

import java.util.Base64
import scala.language.existentials

object ErrorDetailsAnyFormat {

  private val base64enc = Base64.getEncoder.withoutPadding()
  private val base64dec = Base64.getDecoder

  val writer: Writer[ErrorDetailsAny] = { (_, any) =>
    Json.obj(
      "type"  -> Json.fromString(any.`type`),
      "value" -> Json.fromString(base64enc.encodeToString(any.value.toByteArray)),
    )
  }

  val parser: Reader[ErrorDetailsAny] = { (_, json) =>
    json.asObject match {
      case Some(obj) =>
        (obj("type").flatMap(_.asString), obj("value").flatMap(_.asString)) match {
          case (Some(t), Some(v)) =>
            ErrorDetailsAny(t, unsafeWrap(base64dec.decode(v)))
          case _ =>
            throw new JsonFormatException(s"Error parsing ErrorDetailAny: $json")
        }
      case None =>
        throw new JsonFormatException(s"Expected an object, got $json")
    }
  }

}

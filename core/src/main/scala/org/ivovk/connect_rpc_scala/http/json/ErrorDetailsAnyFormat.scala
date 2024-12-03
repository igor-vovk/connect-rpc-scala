package org.ivovk.connect_rpc_scala.http.json


import com.google.protobuf.UnsafeByteOperations.unsafeWrap
import connectrpc.ErrorDetailsAny
import org.json4s.JsonAST.{JObject, JString, JValue}
import org.json4s.MonadicJValue.*
import scalapb.json4s.{JsonFormatException, Parser, Printer}

import java.util.Base64
import scala.language.existentials

object ErrorDetailsAnyFormat {

  private val base64enc = Base64.getEncoder.withoutPadding()
  private val base64dec = Base64.getDecoder

  val writer: (Printer, ErrorDetailsAny) => JValue = { (printer, any) =>
    JObject(
      "type" -> JString(any.`type`),
      "value" -> JString(base64enc.encodeToString(any.value.toByteArray))
    )
  }

  val printer: (Parser, JValue) => ErrorDetailsAny = {
    case (parser, obj@JObject(fields)) =>
      (obj \ "type", obj \ "value") match {
        case (JString(t), JString(v)) =>
          ErrorDetailsAny(t, unsafeWrap(base64dec.decode(v)))
        case _ =>
          throw new JsonFormatException(s"Error parsing ErrorDetailAny: $obj")
      }
    case (parser, other) =>
      throw new JsonFormatException(s"Expected an object, got $other")
  }

}

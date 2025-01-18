package org.ivovk.connect_rpc_scala.http.json

import connectrpc.{Error, ErrorDetailsAny}
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.json4s.MonadicJValue.*
import org.json4s.{JNothing, JObject}
import scalapb.json4s.{Parser, Printer}

object ConnectErrorFormat {

  private val stringErrorCodes: Array[JString] = {
    val maxCode = connectrpc.Code.values.map(_.value).max
    val codes   = new Array[JString](maxCode + 1)

    connectrpc.Code.values.foreach { code =>
      codes(code.value) = JString(code.name.substring("CODE_".length).toLowerCase)
    }

    codes
  }

  val writer: (Printer, Error) => JValue = { (printer, error) =>
    JObject(
      List.concat(
        Some("code" -> stringErrorCodes(error.code.value)),
        error.message.map("message" -> JString(_)),
        Option(error.details).filterNot(_.isEmpty).map(d => "details" -> JArray(d.map(printer.toJson).toList)),
      )
    )
  }

  val parser: (Parser, JValue) => Error = {
    case (parser, obj @ JObject(fields)) =>
      val code = obj \ "code" match
        case JString(code) =>
          connectrpc.Code
            .fromName(s"CODE_${code.toUpperCase}")
            .getOrElse(throw new IllegalArgumentException(s"Unknown error code: $code"))
        case _ => throw new IllegalArgumentException(s"Error parsing Error: $obj")

      val message = obj \ "message" match
        case JString(message) => Some(message)
        case JNothing         => None
        case _                => throw new IllegalArgumentException(s"Error parsing Error: $obj")

      val details = obj \ "details" match
        case JArray(details) => details.map(parser.fromJson[ErrorDetailsAny])
        case JNothing        => Seq.empty
        case _               => throw new IllegalArgumentException(s"Error parsing Error: $obj")

      Error(
        code = code,
        message = message,
        details = details,
      )
    case (_, other) =>
      throw new IllegalArgumentException(s"Expected an object, got $other")
  }

}

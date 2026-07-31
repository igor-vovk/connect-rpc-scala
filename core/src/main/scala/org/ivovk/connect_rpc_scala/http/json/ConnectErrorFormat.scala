package org.ivovk.connect_rpc_scala.http.json

import connectrpc.{Error, ErrorDetailsAny}
import io.circe.Json

object ConnectErrorFormat {

  private val stringErrorCodes: Array[Json] = {
    val maxCode = connectrpc.Code.values.map(_.value).max
    val codes   = new Array[Json](maxCode + 1)

    connectrpc.Code.values.foreach { code =>
      codes(code.value) = Json.fromString(code.name.substring("CODE_".length).toLowerCase)
    }

    codes
  }

  val writer: Writer[Error] = { (printer, error) =>
    Json.obj(
      List.concat(
        Some("code" -> stringErrorCodes(error.code.value)),
        error.message.map("message" -> Json.fromString(_)),
        Option(error.details).filterNot(_.isEmpty).map(d =>
          "details" -> Json.fromValues(d.map(printer.toJson))
        ),
      )*
    )
  }

  val parser: Reader[Error] = { (parser, json) =>
    json.asObject match {
      case Some(obj) =>
        val code = obj("code").flatMap(_.asString) match
          case Some(code) =>
            connectrpc.Code
              .fromName(s"CODE_${code.toUpperCase}")
              .getOrElse(throw new IllegalArgumentException(s"Unknown error code: $code"))
          case None => throw new IllegalArgumentException(s"Error parsing Error: $json")

        val message = obj("message") match
          case Some(value) =>
            value.asString match
              case Some(message) => Some(message)
              case None          => throw new IllegalArgumentException(s"Error parsing Error: $json")
          case None => None

        val details = obj("details") match
          case Some(value) =>
            value.asArray match
              case Some(details) => details.map(parser.fromJson[ErrorDetailsAny])
              case None          => throw new IllegalArgumentException(s"Error parsing Error: $json")
          case None => Seq.empty

        Error(
          code = code,
          message = message,
          details = details,
        )
      case None =>
        throw new IllegalArgumentException(s"Expected an object, got $json")
    }
  }

}

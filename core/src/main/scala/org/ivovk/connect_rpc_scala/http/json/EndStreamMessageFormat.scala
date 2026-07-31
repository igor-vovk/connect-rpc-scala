package org.ivovk.connect_rpc_scala.http.json

import connectrpc.{EndStreamMessage, MetadataEntry}
import io.circe.{Json, JsonObject}

object EndStreamMessageFormat {

  val writer: Writer[EndStreamMessage] = { (printer, message) =>
    Json.obj(
      List.concat(
        message.error.map(error => "error" -> ConnectErrorFormat.writer(printer, error)),
        Option(message.metadata).filterNot(_.isEmpty).map(m => "metadata" -> metadataToJson(m)),
      )*
    )
  }

  val parser: Reader[EndStreamMessage] = { (parser, json) =>
    json.asObject match {
      case Some(obj) =>
        val error = obj("error").map(ConnectErrorFormat.parser(parser, _))

        val metadata = obj("metadata") match
          case Some(value) =>
            value.asObject match
              case Some(fields) => jsonToMetadata(fields)
              case None => throw new IllegalArgumentException(s"Error parsing EndStreamMessage: $json")
          case None => Seq.empty

        EndStreamMessage(
          error = error,
          metadata = metadata,
        )
      case None =>
        throw new IllegalArgumentException(s"Expected an object, got $json")
    }
  }

  private def metadataToJson(metadata: Seq[MetadataEntry]): Json =
    Json.obj(
      metadata.map(entry => entry.key -> Json.fromValues(entry.value.map(Json.fromString)))*
    )

  private def jsonToMetadata(fields: JsonObject): Seq[MetadataEntry] =
    fields.toIterable.map { case (key, value) =>
      val values = value.asArray match
        case Some(arr) =>
          arr.map { json =>
            json.asString.getOrElse(
              throw new IllegalArgumentException(s"Expected string in metadata array, got $json")
            )
          }
        case None =>
          value.asString match
            case Some(s) => Seq(s)
            case None    => throw new IllegalArgumentException("Expected array or string for metadata value")

      MetadataEntry(key = key, value = values)
    }.toSeq

}

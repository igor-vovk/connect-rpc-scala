package org.ivovk.connect_rpc_scala.http.json

import connectrpc.{EndStreamMessage, MetadataEntry}
import org.json4s.JsonAST.{JObject, JString, JValue}
import org.json4s.MonadicJValue.*
import org.json4s.{JArray, JNothing}

object EndStreamMessageFormat {

  val writer: Writer[EndStreamMessage] = { (printer, message) =>
    JObject(
      List.concat(
        message.error.map(error => "error" -> ConnectErrorFormat.writer(printer, error)),
        Option(message.metadata).filterNot(_.isEmpty).map(m => "metadata" -> metadataToJson(m)),
      )
    )
  }

  val parser: Reader[EndStreamMessage] = {
    case (parser, obj @ JObject(fields)) =>
      val error = obj \ "error" match
        case JNothing  => None
        case errorJson => Some(ConnectErrorFormat.parser(parser, errorJson))

      val metadata = obj \ "metadata" match
        case JObject(fields) => jsonToMetadata(fields)
        case JNothing        => Seq.empty
        case _               => throw new IllegalArgumentException(s"Error parsing EndStreamMessage: $obj")

      EndStreamMessage(
        error = error,
        metadata = metadata,
      )
    case (_, other) =>
      throw new IllegalArgumentException(s"Expected an object, got $other")
  }

  private def metadataToJson(metadata: Seq[MetadataEntry]): JValue =
    JObject(
      metadata.map(entry => entry.key -> JArray(entry.value.map(JString(_)).toList)).toList
    )

  private def jsonToMetadata(fields: List[(String, JValue)]): Seq[MetadataEntry] =
    fields.map { case (key, value) =>
      val values = value match
        case JArray(arr) =>
          arr.map {
            case JString(s) => s
            case v => throw new IllegalArgumentException(s"Expected string in metadata array, got $v")
          }
        case JString(s) => Seq(s)
        case _          => throw new IllegalArgumentException(s"Expected array or string for metadata value")

      MetadataEntry(key = key, value = values)
    }

}

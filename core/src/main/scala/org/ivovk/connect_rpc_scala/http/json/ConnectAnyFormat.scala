package org.ivovk.connect_rpc_scala.http.json

import com.google.protobuf.any.Any as PBAny
import org.json4s.JsonAST.{JObject, JString, JValue}
import scalapb.json4s.Printer

import scala.language.existentials

object ConnectAnyFormat {
  // Messages that have special representation are parsed/serialized from a `value` field of the
  // any.
  private val SpecialValues: Set[scalapb.GeneratedMessageCompanion[_]] = (
    com.google.protobuf.struct.StructProto.messagesCompanions ++
      com.google.protobuf.wrappers.WrappersProto.messagesCompanions ++
      Seq(
        com.google.protobuf.any.Any,
        com.google.protobuf.duration.Duration,
        com.google.protobuf.timestamp.Timestamp,
        com.google.protobuf.field_mask.FieldMask
      )
    ).toSet

  val anyWriter: (Printer, PBAny) => JValue = { case (printer, any) =>
    // Find the companion so it can be used to JSON-serialize the message. Perhaps this can be circumvented by
    // including the original GeneratedMessage with the Any (at least in memory).
    val cmp = printer.typeRegistry
      .findType(any.typeUrl)
      .getOrElse(
        throw new IllegalStateException(
          s"Unknown type ${any.typeUrl} in Any.  Add a TypeRegistry that supports this type to the Printer."
        )
      )

    // Unpack the message...
    val message = any.unpack(cmp)

    // ... and add the @type marker to the resulting JSON
    if (SpecialValues.contains(cmp))
      JObject(
        "@type" -> JString(any.typeUrl),
        "value" -> printer.toJson(message)
      )
    else
      printer.toJson(message) match {
        case JObject(fields) =>
          JObject(("@type" -> JString(any.typeUrl)) +: fields)
        case value =>
          // Safety net, this shouldn't happen
          throw new IllegalStateException(
            s"Message of type ${any.typeUrl} emitted non-object JSON: $value"
          )
      }
  }

//  val anyParser: (Parser, JValue) => PBAny = {
//    case (parser, obj @ JObject(fields)) =>
//      obj \ "@type" match {
//        case JString(typeUrl) =>
//          val cmp = parser.typeRegistry
//            .findType(typeUrl)
//            .getOrElse(
//              throw new JsonFormatException(
//                s"Unknown type ${typeUrl} in Any.  Add a TypeRegistry that supports this type to the Parser."
//              )
//            )
//          val input = if (SpecialValues.contains(cmp)) obj \ "value" else obj
//          val message = parser.fromJson(input, true)(cmp)
//          PBAny(typeUrl = typeUrl, value = message.toByteString)
//
//        case JNothing =>
//          throw new JsonFormatException(s"Missing type url when parsing $obj")
//
//        case unknown =>
//          throw new JsonFormatException(
//            s"Expected string @type field, got $unknown"
//          )
//      }
//
//    case (_, unknown) =>
//      throw new JsonFormatException(s"Expected an object, got $unknown")
//  }
}


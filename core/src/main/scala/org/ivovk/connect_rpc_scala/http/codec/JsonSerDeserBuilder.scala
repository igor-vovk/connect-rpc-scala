package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import org.ivovk.connect_rpc_scala.http.json.{ConnectErrorFormat, ErrorDetailsAnyFormat}
import scalapb.json4s.{FormatRegistry, JsonFormat, TypeRegistry}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, json4s}

import scala.util.chaining.*

case class JsonSerDeser[F[_]](
  parser: json4s.Parser,
  //printer: json4s.Printer,
  codec: JsonMessageCodec[F],
)

object JsonSerDeserBuilder {
  def apply[F[_] : Sync](): JsonSerDeserBuilder[F] =
    new JsonSerDeserBuilder(
      typeRegistry = TypeRegistry.default,
      formatRegistry = JsonFormat.DefaultRegistry,
    )
}

case class JsonSerDeserBuilder[F[_] : Sync] private(
  typeRegistry: TypeRegistry,
  formatRegistry: FormatRegistry,
) {

  def registerType[T <: GeneratedMessage](using cmp: GeneratedMessageCompanion[T]): JsonSerDeserBuilder[F] =
    copy(
      typeRegistry = typeRegistry.addMessageByCompanion(cmp),
    )

  def build: JsonSerDeser[F] = {
    val formatRegistry = this.formatRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.parser
      )
      .registerMessageFormatter[connectrpc.Error](
        ConnectErrorFormat.writer,
        ConnectErrorFormat.parser
      )

    val parser = new json4s.Parser()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    val printer = new json4s.Printer()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    JsonSerDeser[F](
      parser = parser,
      //printer = printer,
      codec = new JsonMessageCodec[F](parser, printer)
    )
  }

}

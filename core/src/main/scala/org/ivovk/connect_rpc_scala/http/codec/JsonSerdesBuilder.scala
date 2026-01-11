package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Async
import org.ivovk.connect_rpc_scala.http.json.{ConnectErrorFormat, ErrorDetailsAnyFormat}
import scalapb.json4s.{FormatRegistry, JsonFormat, TypeRegistry}
import scalapb.{json4s, GeneratedMessage, GeneratedMessageCompanion}

case class JsonSerdes[F[_]](
  parser: json4s.Parser,
  codec: JsonMessageCodec[F],
  streamingCodec: JsonStreamingMessageCodec[F],
)

object JsonSerdesBuilder {
  def apply[F[_]: Async](): JsonSerdesBuilder[F] =
    new JsonSerdesBuilder(
      typeRegistry = TypeRegistry.default,
      formatRegistry = JsonFormat.DefaultRegistry,
    )
}

case class JsonSerdesBuilder[F[_]: Async] private (
  typeRegistry: TypeRegistry,
  formatRegistry: FormatRegistry,
) {

  def registerType[T <: GeneratedMessage](using cmp: GeneratedMessageCompanion[T]): JsonSerdesBuilder[F] =
    copy(
      typeRegistry = typeRegistry.addMessageByCompanion(cmp)
    )

  def build: JsonSerdes[F] = {
    val formatRegistry = this.formatRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.parser,
      )
      .registerMessageFormatter[connectrpc.Error](
        ConnectErrorFormat.writer,
        ConnectErrorFormat.parser,
      )

    val parser = new json4s.Parser()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    val printer = new json4s.Printer()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    JsonSerdes[F](
      parser = parser,
      codec = new JsonMessageCodec[F](parser, printer),
      streamingCodec = new JsonStreamingMessageCodec[F](parser, printer),
    )
  }

}

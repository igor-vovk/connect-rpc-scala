package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import org.ivovk.connect_rpc_scala.http.json.{
  ConnectErrorFormat,
  EndStreamMessageFormat,
  ErrorDetailsAnyFormat,
}
import scalapb.{GeneratedMessage => Message, GeneratedMessageCompanion => Companion}
import scalapb_circe.{FormatRegistry, JsonFormat, Parser, Printer}
import scalapb_json.TypeRegistry

case class JsonSerdes[F[_]](
  parser: Parser,
  codec: JsonMessageCodec[F],
  streamingCodec: JsonStreamingMessageCodec[F],
)

object JsonSerdesBuilder {
  def apply[F[_]: Sync](): JsonSerdesBuilder[F] =
    new JsonSerdesBuilder(
      typeRegistry = TypeRegistry.empty,
      formatRegistry = JsonFormat.DefaultRegistry,
    )
}

case class JsonSerdesBuilder[F[_]: Sync] private (
  typeRegistry: TypeRegistry,
  formatRegistry: FormatRegistry,
) {

  def registerType[T <: Message](using cmp: Companion[T]): JsonSerdesBuilder[F] =
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
      .registerMessageFormatter[connectrpc.EndStreamMessage](
        EndStreamMessageFormat.writer,
        EndStreamMessageFormat.parser,
      )

    val parser  = new Parser(false, false, formatRegistry, typeRegistry)
    val printer = new Printer(false, false, false, false, false, formatRegistry, typeRegistry)

    JsonSerdes[F](
      parser = parser,
      codec = new JsonMessageCodec[F](parser, printer),
      streamingCodec = new JsonStreamingMessageCodec[F](parser, printer),
    )
  }

}

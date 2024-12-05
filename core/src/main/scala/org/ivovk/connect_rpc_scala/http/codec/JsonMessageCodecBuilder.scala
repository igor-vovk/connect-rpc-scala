package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import org.ivovk.connect_rpc_scala.http.json.ErrorDetailsAnyFormat
import scalapb.json4s.{FormatRegistry, JsonFormat, TypeRegistry}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, json4s}

import scala.util.chaining.*

object JsonMessageCodecBuilder {
  def apply[F[_] : Sync](): JsonMessageCodecBuilder[F] =
    new JsonMessageCodecBuilder(
      typeRegistry = TypeRegistry.default,
      formatRegistry = JsonFormat.DefaultRegistry,
    )
}

case class JsonMessageCodecBuilder[F[_] : Sync] private(
  typeRegistry: TypeRegistry,
  formatRegistry: FormatRegistry,
) {

  def registerType[T <: GeneratedMessage](using cmp: GeneratedMessageCompanion[T]): JsonMessageCodecBuilder[F] =
    copy(
      typeRegistry = typeRegistry.addMessageByCompanion(cmp),
    )

  def build: JsonMessageCodec[F] = {
    val formatRegistry = this.formatRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.printer
      )

    val parser = new json4s.Parser()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    val printer = new json4s.Printer()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    JsonMessageCodec[F](parser, printer)
  }

}

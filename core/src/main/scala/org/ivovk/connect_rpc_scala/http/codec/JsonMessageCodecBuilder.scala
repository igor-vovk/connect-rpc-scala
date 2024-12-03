package org.ivovk.connect_rpc_scala.http.codec

import cats.Endo
import cats.effect.Sync
import org.ivovk.connect_rpc_scala.http.json.ErrorDetailsAnyFormat
import scalapb.json4s

import scala.util.chaining.*

object JsonMessageCodecBuilder {
  def apply[F[_] : Sync](): JsonMessageCodecBuilder[F] = new JsonMessageCodecBuilder()
}

case class JsonMessageCodecBuilder[F[_] : Sync] private(
  typeRegistryConfigurator: Endo[json4s.TypeRegistry] = identity,
  formatRegistryConfigurator: Endo[json4s.FormatRegistry] = identity,
) {

  def withTypeRegistryConfigurator(method: Endo[json4s.TypeRegistry]): JsonMessageCodecBuilder[F] =
    copy(typeRegistryConfigurator = method)

  def withFormatRegistryConfigurator(method: Endo[json4s.FormatRegistry]): JsonMessageCodecBuilder[F] =
    copy(formatRegistryConfigurator = method)

  def build: JsonMessageCodec[F] = {
    val typeRegistry = json4s.TypeRegistry.default
      .pipe(typeRegistryConfigurator)

    val formatRegistry = json4s.JsonFormat.DefaultRegistry
      .registerMessageFormatter[connectrpc.ErrorDetailsAny](
        ErrorDetailsAnyFormat.writer,
        ErrorDetailsAnyFormat.printer
      )
      .pipe(formatRegistryConfigurator)

    val parser = new json4s.Parser()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    val printer = new json4s.Printer()
      .withTypeRegistry(typeRegistry)
      .withFormatRegistry(formatRegistry)

    JsonMessageCodec[F](parser, printer)
  }

}

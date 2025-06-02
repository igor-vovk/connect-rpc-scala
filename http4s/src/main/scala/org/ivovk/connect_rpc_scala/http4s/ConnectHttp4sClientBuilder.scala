package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import io.grpc.Channel
import org.http4s.Uri
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.{HeaderMapping, HeadersFilter, MediaTypes}
import org.ivovk.connect_rpc_scala.http.codec.{
  JsonSerDeser,
  JsonSerDeserBuilder,
  MessageCodecRegistry,
  ProtoMessageCodec,
}
import org.ivovk.connect_rpc_scala.http4s.client.Http4sChannel

object ConnectHttp4sClientBuilder {

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(
      client = client,
      customJsonSerDeser = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
    )
}

class ConnectHttp4sClientBuilder[F[_]: Async] private (
  client: Client[F],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
) {

  private def copy(
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    incomingHeadersFilter: HeadersFilter = incomingHeadersFilter,
    outgoingHeadersFilter: HeadersFilter = outgoingHeadersFilter,
  ): ConnectHttp4sClientBuilder[F] =
    new ConnectHttp4sClientBuilder(
      client,
      customJsonSerDeser,
      incomingHeadersFilter,
      outgoingHeadersFilter,
    )

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectHttp4sClientBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  /**
   * Filter for incoming headers.
   *
   * By default, headers with "connection" prefix are filtered out (GRPC requirement).
   */
  def withIncomingHeadersFilter(filter: String => Boolean): ConnectHttp4sClientBuilder[F] =
    copy(incomingHeadersFilter = filter)

  /**
   * Filter for outgoing headers.
   *
   * By default, headers with "grpc-" prefix are filtered out.
   */
  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectHttp4sClientBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  def build(baseUri: Uri): Resource[F, Channel] =
    for dispatcher <- Dispatcher.parallel[F](await = false)
    yield {
      val jsonSerDeser = customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build)
      val codecRegistry = MessageCodecRegistry[F](
        jsonSerDeser.codec,
        ProtoMessageCodec[F](),
      )

      val headerMapping = Http4sHeaderMapping(
        incomingHeadersFilter,
        outgoingHeadersFilter,
        treatTrailersAsHeaders = true,
      )

      new Http4sChannel(
        client,
        dispatcher,
        codecRegistry.byMediaType(MediaTypes.`application/json`).get,
        headerMapping,
        baseUri,
      )
    }

}

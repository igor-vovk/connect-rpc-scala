package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.std.Dispatcher
import cats.effect.{Async, Resource}
import io.grpc.Channel
import org.http4s.Uri
import org.http4s.client.Client
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerDeser, JsonSerDeserBuilder, ProtoMessageCodec}
import org.ivovk.connect_rpc_scala.http.{HeaderMapping, HeadersFilter}
import org.ivovk.connect_rpc_scala.http4s.client.ConnectHttp4sChannel

object ConnectHttp4sChannelBuilder {

  def apply[F[_]: Async](client: Client[F]): ConnectHttp4sChannelBuilder[F] =
    new ConnectHttp4sChannelBuilder(
      client = client,
      customJsonSerDeser = None,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      useBinaryFormat = false,
    )
}

class ConnectHttp4sChannelBuilder[F[_]: Async] private (
  client: Client[F],
  customJsonSerDeser: Option[JsonSerDeser[F]],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
  useBinaryFormat: Boolean,
) {

  private def copy(
    customJsonSerDeser: Option[JsonSerDeser[F]] = customJsonSerDeser,
    incomingHeadersFilter: HeadersFilter = incomingHeadersFilter,
    outgoingHeadersFilter: HeadersFilter = outgoingHeadersFilter,
    useBinaryFormat: Boolean = useBinaryFormat,
  ): ConnectHttp4sChannelBuilder[F] =
    new ConnectHttp4sChannelBuilder(
      client,
      customJsonSerDeser,
      incomingHeadersFilter,
      outgoingHeadersFilter,
      useBinaryFormat,
    )

  def withJsonCodecConfigurator(method: Endo[JsonSerDeserBuilder[F]]): ConnectHttp4sChannelBuilder[F] =
    copy(customJsonSerDeser = Some(method(JsonSerDeserBuilder[F]()).build))

  /**
   * Filter for incoming headers.
   *
   * By default, headers with "connection" prefix are filtered out (GRPC requirement).
   */
  def withIncomingHeadersFilter(filter: String => Boolean): ConnectHttp4sChannelBuilder[F] =
    copy(incomingHeadersFilter = filter)

  /**
   * Filter for outgoing headers.
   *
   * By default, headers with "grpc-" prefix are filtered out.
   */
  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectHttp4sChannelBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  /**
   * Use protobuf binary format for messages.
   *
   * By default, JSON format is used. It is marked as experimental, so use it with caution. Only JSON format
   * is guaranteed to be stable for now.
   */
  def enableBinaryFormat(): ConnectHttp4sChannelBuilder[F] =
    copy(useBinaryFormat = true)

  def build(baseUri: Uri): Resource[F, Channel] =
    for dispatcher <- Dispatcher.parallel[F](await = false)
    yield {
      val codec =
        if useBinaryFormat then ProtoMessageCodec[F]()
        else customJsonSerDeser.getOrElse(JsonSerDeserBuilder[F]().build).codec

      val headerMapping = Http4sHeaderMapping(
        incomingHeadersFilter,
        outgoingHeadersFilter,
        treatTrailersAsHeaders = true,
      )

      new ConnectHttp4sChannel(
        httpClient = client,
        dispatcher = dispatcher,
        messageCodec = codec,
        headerMapping = headerMapping,
        baseUri = baseUri,
      )
    }

}

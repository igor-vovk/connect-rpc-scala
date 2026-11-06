package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.{Async, Resource}
import cats.implicits.*
import org.http4s.{HttpApp, HttpRoutes, Uri}
import org.ivovk.connect_rpc_scala.http.{HeaderMapping, HeadersFilter}
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerdes, JsonSerdesBuilder}
import org.ivovk.connect_rpc_scala.service.ServiceDefinition

object ConnectHttp4sRouteBuilder {

  def forService[F[_]: Async](service: ServiceDefinition[F]): ConnectHttp4sRouteBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_]: Async](
    service: ServiceDefinition[F],
    other: ServiceDefinition[F]*
  ): ConnectHttp4sRouteBuilder[F] =
    forServices(service +: other)

  def forServices[F[_]: Async](services: Seq[ServiceDefinition[F]]): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services = services,
      jsonSerdes = JsonSerdesBuilder[F]().build,
      incomingHeadersFilter = HeaderMapping.DefaultIncomingHeadersFilter,
      outgoingHeadersFilter = HeaderMapping.DefaultOutgoingHeadersFilter,
      pathPrefix = Uri.Path.Root,
      treatTrailersAsHeaders = true,
      additionalRoutes = None,
    )
}

final class ConnectHttp4sRouteBuilder[F[_]: Async] private[http4s] (
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  incomingHeadersFilter: HeadersFilter,
  outgoingHeadersFilter: HeadersFilter,
  pathPrefix: Uri.Path,
  treatTrailersAsHeaders: Boolean,
  additionalRoutes: Option[HttpRoutes[F]],
) {

  private def copy(
    services: Seq[ServiceDefinition[F]] = services,
    jsonSerdes: JsonSerdes[F] = jsonSerdes,
    incomingHeadersFilter: String => Boolean = incomingHeadersFilter,
    outgoingHeadersFilter: String => Boolean = outgoingHeadersFilter,
    pathPrefix: Uri.Path = pathPrefix,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
    additionalRoutes: Option[HttpRoutes[F]] = additionalRoutes,
  ): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services,
      jsonSerdes,
      incomingHeadersFilter,
      outgoingHeadersFilter,
      pathPrefix,
      treatTrailersAsHeaders,
      additionalRoutes,
    )

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sRouteBuilder[F] =
    copy(jsonSerdes = configure(JsonSerdesBuilder[F]()).build)

  def withIncomingHeadersFilter(filter: String => Boolean): ConnectHttp4sRouteBuilder[F] =
    copy(incomingHeadersFilter = filter)

  def withOutgoingHeadersFilter(filter: String => Boolean): ConnectHttp4sRouteBuilder[F] =
    copy(outgoingHeadersFilter = filter)

  def withPathPrefix(path: Uri.Path): ConnectHttp4sRouteBuilder[F] =
    copy(pathPrefix = path)

  def disableTreatingTrailersAsHeaders: ConnectHttp4sRouteBuilder[F] =
    copy(treatTrailersAsHeaders = false)

  def withAdditionalRoutes(routes: HttpRoutes[F]): ConnectHttp4sRouteBuilder[F] =
    copy(additionalRoutes = Some(routes))

  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map { routes =>
      additionalRoutes.fold(routes.routes)(routes.routes <+> _).orNotFound
    }

  def buildRoutes: Resource[F, ConnectHttp4sRoutes[F]] = {
    val unaryHandler = ConnectHttp4sUnaryHandler(
      incomingHeadersFilter,
      outgoingHeadersFilter,
      treatTrailersAsHeaders,
    )
    val connectRoutes =
      ConnectHttp4sConnectRoutes(services, jsonSerdes, pathPrefix, unaryHandler).routes
    val transcodingRoutes =
      ConnectHttp4sTranscodingRoutes(services, jsonSerdes, pathPrefix, unaryHandler).routes

    Resource.pure(ConnectHttp4sRoutes(connectRoutes, transcodingRoutes))
  }
}

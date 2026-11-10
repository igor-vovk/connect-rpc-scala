package org.ivovk.connect_rpc_scala.http4s

import cats.Endo
import cats.effect.{Async, Resource}
import cats.implicits.*
import org.http4s.{HttpApp, HttpRoutes, Uri}
import org.ivovk.connect_rpc_scala.http.codec.{JsonSerdes, JsonSerdesBuilder}
import org.ivovk.connect_rpc_scala.http4s.server.{ConnectRoutes, Routes, TranscodingRoutes, UnaryHandler}
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
      pathPrefix = Uri.Path.Root,
      treatTrailersAsHeaders = true,
      additionalRoutes = None,
    )
}

final class ConnectHttp4sRouteBuilder[F[_]: Async] private[http4s] (
  services: Seq[ServiceDefinition[F]],
  jsonSerdes: JsonSerdes[F],
  pathPrefix: Uri.Path,
  treatTrailersAsHeaders: Boolean,
  additionalRoutes: Option[HttpRoutes[F]],
) {

  private def copy(
    services: Seq[ServiceDefinition[F]] = services,
    jsonSerdes: JsonSerdes[F] = jsonSerdes,
    pathPrefix: Uri.Path = pathPrefix,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
    additionalRoutes: Option[HttpRoutes[F]] = additionalRoutes,
  ): ConnectHttp4sRouteBuilder[F] =
    new ConnectHttp4sRouteBuilder(
      services,
      jsonSerdes,
      pathPrefix,
      treatTrailersAsHeaders,
      additionalRoutes,
    )

  def withJsonCodecConfigurator(
    configure: Endo[JsonSerdesBuilder[F]]
  ): ConnectHttp4sRouteBuilder[F] =
    copy(jsonSerdes = configure(JsonSerdesBuilder[F]()).build)

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

  def buildRoutes: Resource[F, Routes[F]] = {
    val unaryHandler  = UnaryHandler(treatTrailersAsHeaders, jsonSerdes.codec)
    val connectRoutes =
      ConnectRoutes(services, jsonSerdes, pathPrefix, unaryHandler).routes
    val transcodingRoutes =
      TranscodingRoutes(services, jsonSerdes, pathPrefix, unaryHandler).routes

    Resource.pure(Routes(connectRoutes, transcodingRoutes))
  }
}

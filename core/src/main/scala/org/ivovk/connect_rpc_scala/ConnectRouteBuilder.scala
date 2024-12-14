package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.data.OptionT
import cats.effect.{Async, Resource}
import cats.implicits.*
import io.grpc.{ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import org.http4s.Status.*
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpApp, HttpRoutes, MediaType, Method, Response, Uri}
import org.ivovk.connect_rpc_scala.grpc.*
import org.ivovk.connect_rpc_scala.grpc.MergingBuilder.*
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.QueryParams.*
import org.ivovk.connect_rpc_scala.http.codec.*
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object ConnectRouteBuilder {

  def forService[F[_] : Async](service: ServerServiceDefinition): ConnectRouteBuilder[F] =
    forServices(Seq(service))

  def forServices[F[_] : Async](service: ServerServiceDefinition, other: ServerServiceDefinition*): ConnectRouteBuilder[F] =
    forServices(service +: other)

  def forServices[F[_] : Async](services: Seq[ServerServiceDefinition]): ConnectRouteBuilder[F] =
    new ConnectRouteBuilder(
      services = services,
      serverConfigurator = identity,
      channelConfigurator = identity,
      customJsonCodec = None,
      pathPrefix = Uri.Path.Root,
      executor = ExecutionContext.global,
      waitForShutdown = 5.seconds,
      treatTrailersAsHeaders = true,
    )

}

final class ConnectRouteBuilder[F[_] : Async] private(
  services: Seq[ServerServiceDefinition],
  serverConfigurator: Endo[ServerBuilder[_]],
  channelConfigurator: Endo[ManagedChannelBuilder[_]],
  customJsonCodec: Option[JsonMessageCodec[F]],
  pathPrefix: Uri.Path,
  executor: Executor,
  waitForShutdown: Duration,
  treatTrailersAsHeaders: Boolean,
) {

  private def copy(
    services: Seq[ServerServiceDefinition] = services,
    serverConfigurator: Endo[ServerBuilder[_]] = serverConfigurator,
    channelConfigurator: Endo[ManagedChannelBuilder[_]] = channelConfigurator,
    customJsonCodec: Option[JsonMessageCodec[F]] = customJsonCodec,
    pathPrefix: Uri.Path = pathPrefix,
    executor: Executor = executor,
    waitForShutdown: Duration = waitForShutdown,
    treatTrailersAsHeaders: Boolean = treatTrailersAsHeaders,
  ): ConnectRouteBuilder[F] =
    new ConnectRouteBuilder(
      services,
      serverConfigurator,
      channelConfigurator,
      customJsonCodec,
      pathPrefix,
      executor,
      waitForShutdown,
      treatTrailersAsHeaders,
    )

  def withServerConfigurator(method: Endo[ServerBuilder[_]]): ConnectRouteBuilder[F] =
    copy(serverConfigurator = method)

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectRouteBuilder[F] =
    copy(channelConfigurator = method)

  def withJsonCodecConfigurator(method: Endo[JsonMessageCodecBuilder[F]]): ConnectRouteBuilder[F] =
    copy(customJsonCodec = Some(method(JsonMessageCodecBuilder[F]()).build))

  def withPathPrefix(path: Uri.Path): ConnectRouteBuilder[F] =
    copy(pathPrefix = path)

  def withExecutor(executor: Executor): ConnectRouteBuilder[F] =
    copy(executor = executor)

  def withWaitForShutdown(duration: Duration): ConnectRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * When enabled, response trailers are treated as headers (no "trailer-" prefix added).
   *
   * Both `fs2-grpc` and `zio-grpc` support trailing headers only, so enabling this option is a single way to
   * send headers from the server to a client.
   *
   * Enabled by default.
   */
  def withTreatTrailersAsHeaders(enabled: Boolean): ConnectRouteBuilder[F] =
    copy(treatTrailersAsHeaders = enabled)

  /**
   * Use this method only if you want to add additional routes to the server.
   *
   * Otherwise, [[build]] method should be preferred.
   */
  def buildRoutes: Resource[F, HttpRoutes[F]] = {
    val httpDsl = Http4sDsl[F]
    import httpDsl.*

    val jsonCodec     = customJsonCodec.getOrElse(JsonMessageCodecBuilder[F]().build)
    val codecRegistry = MessageCodecRegistry[F](
      jsonCodec,
      ProtoMessageCodec[F](),
    )

    val methodRegistry = MethodRegistry(services)

    for
      channel <- InProcessChannelBridge.create(
        services,
        serverConfigurator,
        channelConfigurator,
        executor,
        waitForShutdown,
      )
    yield
      val errorHandler = new ConnectErrorHandler[F](
        treatTrailersAsHeaders,
      )

      val connectHandler = new ConnectHandler(
        channel,
        errorHandler,
        treatTrailersAsHeaders,
      )

      val connectRoutes = HttpRoutes[F] {
        case req@Method.GET -> `pathPrefix` / service / method :? EncodingQP(mediaType) +& MessageQP(message) =>
          OptionT.fromOption[F](methodRegistry.get(service, method))
            // Temporary support GET-requests for all methods,
            // until https://github.com/scalapb/ScalaPB/pull/1774 is merged
            .filter(_.descriptor.isSafe || true)
            .semiflatMap { methodEntry =>
              withCodec(codecRegistry, mediaType.some) { codec =>
                val entity = RequestEntity[F](message, req.headers)

                connectHandler.handle(entity, methodEntry)(using codec)
              }
            }
        case req@Method.POST -> `pathPrefix` / service / method =>
          OptionT.fromOption[F](methodRegistry.get(service, method))
            .semiflatMap { methodEntry =>
              withCodec(codecRegistry, req.contentType.map(_.mediaType)) { codec =>
                val entity = RequestEntity[F](req.body, req.headers)

                connectHandler.handle(entity, methodEntry)(using codec)
              }
            }
        case _ =>
          OptionT.none
      }

      val transcodingUrlMatcher = TranscodingUrlMatcher.create[F](
        methodRegistry.all,
        pathPrefix,
      )

      val transcodingHandler = new TranscodingHandler(
        channel,
        errorHandler,
      )

      val transcodingRoutes = HttpRoutes[F] { req =>
        OptionT.fromOption[F](transcodingUrlMatcher.matchesRequest(req))
          .semiflatMap { case MatchedRequest(method, pathJson, queryJson) =>
            given MessageCodec[F] = jsonCodec

            given Companion[Message] = method.requestMessageCompanion

            RequestEntity[F](req.body, req.headers).as[Message]
              .flatMap { bodyMessage =>
                val pathMessage  = jsonCodec.parser.fromJson[Message](pathJson)
                val queryMessage = jsonCodec.parser.fromJson[Message](queryJson)

                transcodingHandler.handleUnary(
                  bodyMessage.merge(pathMessage).merge(queryMessage).build,
                  req.headers,
                  method
                )
              }
          }
      }

      connectRoutes <+> transcodingRoutes
  }

  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map(_.orNotFound)

  private def withCodec(
    registry: MessageCodecRegistry[F],
    mediaType: Option[MediaType]
  )(r: MessageCodec[F] => F[Response[F]]): F[Response[F]] = {
    mediaType.flatMap(registry.byMediaType) match {
      case Some(codec) => r(codec)
      case None =>
        val message = s"Unsupported media-type ${mediaType.show}. " +
          s"Supported media types: ${MediaTypes.allSupported.map(_.show).mkString(", ")}"

        Response(UnsupportedMediaType).withEntity(message).pure[F]
    }
  }

}

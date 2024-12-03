package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.effect.{Async, Resource}
import cats.implicits.*
import io.grpc.{ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpApp, HttpRoutes, Method, Uri}
import org.ivovk.connect_rpc_scala.grpc.*
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.QueryParams.*
import org.ivovk.connect_rpc_scala.http.json.ConnectJsonRegistry
import scalapb.json4s.{JsonFormat, Printer}

import java.util.concurrent.Executor
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.chaining.*

object ConnectRouteBuilder {

  def forService[F[_] : Async](service: ServerServiceDefinition): ConnectRouteBuilder[F] =
    ConnectRouteBuilder(Seq(service))

  def forServices[F[_] : Async](service: ServerServiceDefinition, other: ServerServiceDefinition*): ConnectRouteBuilder[F] =
    ConnectRouteBuilder(service +: other)

  def forServices[F[_] : Async](services: Seq[ServerServiceDefinition]): ConnectRouteBuilder[F] =
    ConnectRouteBuilder(services)

}

case class ConnectRouteBuilder[F[_] : Async] private(
  services: Seq[ServerServiceDefinition],
  jsonPrinterConfigurator: Endo[Printer] = identity,
  serverConfigurator: Endo[ServerBuilder[_]] = identity,
  channelConfigurator: Endo[ManagedChannelBuilder[_]] = identity,
  pathPrefix: Uri.Path = Uri.Path.Root,
  executor: Executor = ExecutionContext.global,
  waitForShutdown: Duration = 5.seconds,
  treatTrailersAsHeaders: Boolean = true,
) {

  import Mappings.*

  def withJsonPrinterConfigurator(method: Endo[Printer]): ConnectRouteBuilder[F] =
    copy(jsonPrinterConfigurator = method)

  def withServerConfigurator(method: Endo[ServerBuilder[_]]): ConnectRouteBuilder[F] =
    copy(serverConfigurator = method)

  def withChannelConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectRouteBuilder[F] =
    copy(channelConfigurator = method)

  def withPathPrefix(path: Uri.Path): ConnectRouteBuilder[F] =
    copy(pathPrefix = path)

  def withExecutor(executor: Executor): ConnectRouteBuilder[F] =
    copy(executor = executor)

  def withWaitForShutdown(duration: Duration): ConnectRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * If enabled, trailers will be treated as headers (no "trailer-" prefix).
   *
   * Both `fs2-grpc` and `zio-grpc` support trailing headers only, so enabling this option is a single way to
   * send headers from the server to the client.
   *
   * Enabled by default.
   */
  def withTreatTrailersAsHeaders(enabled: Boolean): ConnectRouteBuilder[F] =
    copy(treatTrailersAsHeaders = enabled)

  /**
   * Method can be used if you want to add additional routes to the server.
   * Otherwise, it is preferred to use the [[build]] method.
   */
  def buildRoutes: Resource[F, HttpRoutes[F]] = {
    val httpDsl = Http4sDsl[F]
    import httpDsl.*

    val compressor  = Compressor[F]
    val jsonPrinter = JsonFormat.printer
      .withFormatRegistry(ConnectJsonRegistry.default)
      .pipe(jsonPrinterConfigurator)

    val codecRegistry = MessageCodecRegistry[F](
      JsonMessageCodec[F](compressor, jsonPrinter),
      ProtoMessageCodec[F](compressor)
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
      val handler = new ConnectHandler(
        codecRegistry,
        methodRegistry,
        channel,
        httpDsl,
        treatTrailersAsHeaders,
      )

      HttpRoutes.of[F] {
        case req@Method.GET -> pathPrefix / serviceName / methodName :? EncodingQP(contentType) +& MessageQP(message) =>
          val grpcMethod = MethodName(serviceName, methodName)
          val entity     = RequestEntity[F](message, req.headers)

          handler.handle(Method.GET, contentType.some, entity, grpcMethod)
        case req@Method.POST -> pathPrefix / serviceName / methodName =>
          val grpcMethod  = MethodName(serviceName, methodName)
          val contentType = req.contentType.map(_.mediaType)
          val entity      = RequestEntity[F](req)

          handler.handle(Method.POST, contentType, entity, grpcMethod)
      }
  }

  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map(_.orNotFound)

}

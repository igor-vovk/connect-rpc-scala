package org.ivovk.connect_rpc_scala

import cats.Endo
import cats.effect.{Async, Resource}
import cats.implicits.*
import fs2.compression.Compression
import io.grpc.{ManagedChannelBuilder, ServerBuilder, ServerServiceDefinition}
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpApp, HttpRoutes, Method}
import org.ivovk.connect_rpc_scala.http.*
import org.ivovk.connect_rpc_scala.http.QueryParams.*
import scalapb.json4s.{JsonFormat, Printer}

import scala.concurrent.duration.*

object ConnectRouteBuilder {

  def forService[F[_] : Async: Compression](service: ServerServiceDefinition): ConnectRouteBuilder[F] =
    ConnectRouteBuilder(Seq(service))

  def forServices[F[_] : Async: Compression](service: ServerServiceDefinition, other: ServerServiceDefinition*): ConnectRouteBuilder[F] =
    ConnectRouteBuilder(service +: other)

  def forServices[F[_] : Async: Compression](services: Seq[ServerServiceDefinition]): ConnectRouteBuilder[F] =
    ConnectRouteBuilder(services)

}

case class ConnectRouteBuilder[F[_] : Async: Compression] private(
  services: Seq[ServerServiceDefinition],
  jsonPrinterConfigurator: Endo[Printer] = identity,
  serverBuilderConfigurator: Endo[ServerBuilder[_]] = identity,
  channelBuilderConfigurator: Endo[ManagedChannelBuilder[_]] = identity,
  waitForShutdown: Duration = 5.seconds,
) {

  import Mappings.*

  def withJsonPrinterConfigurator(method: Endo[Printer]): ConnectRouteBuilder[F] =
    copy(jsonPrinterConfigurator = method)

  def withServerBuilderConfigurator(method: Endo[ServerBuilder[_]]): ConnectRouteBuilder[F] =
    copy(serverBuilderConfigurator = method)

  def withChannelBuilderConfigurator(method: Endo[ManagedChannelBuilder[_]]): ConnectRouteBuilder[F] =
    copy(channelBuilderConfigurator = method)

  def withWaitForShutdown(duration: Duration): ConnectRouteBuilder[F] =
    copy(waitForShutdown = duration)

  /**
   * Method can be used if you want to add additional routes to the server.
   * Otherwise, it is preferred to use the [[build]] method.
   */
  def buildRoutes: Resource[F, HttpRoutes[F]] = {
    val httpDsl = Http4sDsl[F]
    import httpDsl.*

    val jsonPrinter = jsonPrinterConfigurator(JsonFormat.printer)

    val codecRegistry = MessageCodecRegistry[F](
      JsonMessageCodec[F](jsonPrinter),
      ProtoMessageCodec[F],
    )

    val methodRegistry = MethodRegistry(services)

    for
      channel <- InProcessChannelBridge.create(
        services,
        serverBuilderConfigurator,
        channelBuilderConfigurator,
        waitForShutdown,
      )
    yield
      val handler = new ConnectHandler(
        codecRegistry,
        methodRegistry,
        channel,
        httpDsl,
      )

      HttpRoutes.of[F] {
        case req@Method.GET -> Root / serviceName / methodName :? EncodingQP(contentType) +& MessageQP(message) =>
          val grpcMethod = grpcMethodName(serviceName, methodName)
          val entity     = RequestEntity[F](message, req.headers)

          handler.handle(Method.GET, contentType.some, entity, grpcMethod)
        case req@Method.POST -> Root / serviceName / methodName =>
          val grpcMethod  = grpcMethodName(serviceName, methodName)
          val contentType = req.contentType.map(_.mediaType)
          val entity      = RequestEntity[F](req)

          handler.handle(Method.POST, contentType, entity, grpcMethod)
      }
  }

  def build: Resource[F, HttpApp[F]] =
    buildRoutes.map(_.orNotFound)

  private inline def grpcMethodName(service: String, method: String): String =
    service + "/" + method

}

package me.ivovk.connect_rpc_scala.service

import com.google.api.http.HttpRule
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.concurrent.duration.FiniteDuration

final case class ConnectRequestContext(
  headers: ConnectHeaders = ConnectHeaders.empty,
  timeout: Option[FiniteDuration] = None,
)

final case class ConnectResponse[A](
  message: A,
  headers: ConnectHeaders = ConnectHeaders.empty,
  trailers: ConnectHeaders = ConnectHeaders.empty,
)

final case class ConnectException(
  code: connectrpc.Code,
  message: String,
  details: Seq[connectrpc.ErrorDetailsAny] = Seq.empty,
  headers: ConnectHeaders = ConnectHeaders.empty,
  trailers: ConnectHeaders = ConnectHeaders.empty,
) extends RuntimeException(message) {

  private[connect_rpc_scala] def withCause(cause: Throwable): ConnectException = {
    initCause(cause)
    this
  }
}

final case class UnaryMethodDescriptor[
  Request <: GeneratedMessage,
  Response <: GeneratedMessage,
](
  serviceName: String,
  methodName: String,
  requestCompanion: GeneratedMessageCompanion[Request],
  responseCompanion: GeneratedMessageCompanion[Response],
  httpRule: Option[HttpRule] = None,
) {
  val procedure: String = s"/$serviceName/$methodName"
}

sealed trait UnaryMethod[F[_]] {
  type RequestMessage <: GeneratedMessage
  type ResponseMessage <: GeneratedMessage

  def descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage]

  def invoke(
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]]
}

object UnaryMethod {

  def apply[
    F[_],
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    methodDescriptor: UnaryMethodDescriptor[Request, Response],
    handler: (Request, ConnectRequestContext) => F[ConnectResponse[Response]],
  ): UnaryMethod[F] =
    new UnaryMethod[F] {
      override type RequestMessage  = Request
      override type ResponseMessage = Response

      override val descriptor: UnaryMethodDescriptor[Request, Response] = methodDescriptor

      override def invoke(
        request: Request,
        ctx: ConnectRequestContext,
      ): F[ConnectResponse[Response]] =
        handler(request, ctx)
    }
}

final case class ServiceDefinition[F[_]](
  name: String,
  methods: Seq[UnaryMethod[F]],
) {
  require(methods.forall(_.descriptor.serviceName == name), "All methods must belong to the service")
}

trait UnaryTransport[F[_]] {

  def unary[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[Request, Response],
    request: Request,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[Response]]
}

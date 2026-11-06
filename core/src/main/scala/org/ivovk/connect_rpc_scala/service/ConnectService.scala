package org.ivovk.connect_rpc_scala.service

import com.google.api.http.HttpRule
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util.Locale
import scala.concurrent.duration.FiniteDuration

final case class ConnectHeaders private (values: Map[String, Vector[String]]) {

  def get(name: String): Option[Vector[String]] =
    values.get(ConnectHeaders.normalize(name))

  def add(name: String, value: String): ConnectHeaders = {
    val normalized = ConnectHeaders.normalize(name)
    copy(values = values.updatedWith(normalized)(existing => Some(existing.getOrElse(Vector.empty) :+ value)))
  }
}

object ConnectHeaders {
  val empty: ConnectHeaders = ConnectHeaders(Map.empty)

  def fromPairs(headers: Iterable[(String, String)]): ConnectHeaders =
    headers.foldLeft(empty) { case (result, (name, value)) => result.add(name, value) }

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)
}

final case class ConnectRequest[A](
  message: A,
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
  cause: Throwable = null,
) extends RuntimeException(message, cause)

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

  def invoke(request: ConnectRequest[RequestMessage]): F[ConnectResponse[ResponseMessage]]
}

object UnaryMethod {

  def apply[
    F[_],
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    methodDescriptor: UnaryMethodDescriptor[Request, Response],
    handler: ConnectRequest[Request] => F[ConnectResponse[Response]],
  ): UnaryMethod[F] =
    new UnaryMethod[F] {
      override type RequestMessage  = Request
      override type ResponseMessage = Response

      override val descriptor: UnaryMethodDescriptor[Request, Response] = methodDescriptor

      override def invoke(request: ConnectRequest[Request]): F[ConnectResponse[Response]] =
        handler(request)
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
    request: ConnectRequest[Request],
  ): F[ConnectResponse[Response]]
}

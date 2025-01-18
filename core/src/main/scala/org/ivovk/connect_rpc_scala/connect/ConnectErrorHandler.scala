package org.ivovk.connect_rpc_scala.connect

import cats.Applicative
import cats.implicits.*
import io.grpc.{Metadata, StatusException, StatusRuntimeException}
import org.http4s.{MessageFailure, Response}
import org.ivovk.connect_rpc_scala.Mappings.*
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec
import org.ivovk.connect_rpc_scala.{ErrorHandler, HeaderMapping}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.*

class ConnectErrorHandler[F[_]: Applicative](
  headerMapping: HeaderMapping
) extends ErrorHandler[F] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def handle(e: Throwable)(using MessageCodec[F]): F[Response[F]] = {
    val grpcStatus = e match {
      case e: StatusException =>
        e.getStatus.getDescription match {
          case "an implementation is missing" => io.grpc.Status.UNIMPLEMENTED
          case _                              => e.getStatus
        }
      case e: StatusRuntimeException => e.getStatus
      case _: MessageFailure         => io.grpc.Status.INVALID_ARGUMENT
      case _                         => io.grpc.Status.INTERNAL
    }

    val (message, metadata) = e match {
      case e: StatusRuntimeException => (Option(e.getStatus.getDescription), e.getTrailers)
      case e: StatusException        => (Option(e.getStatus.getDescription), e.getTrailers)
      case e                         => (Option(e.getMessage), new Metadata())
    }

    val httpStatus  = grpcStatus.toHttpStatus
    val connectCode = grpcStatus.toConnectCode

    // Should be called before converting metadata to headers
    val details = Option(metadata.removeAll(GrpcHeaders.ErrorDetailsKey))
      .fold(Seq.empty)(_.asScala.toSeq)

    val headers = headerMapping.trailersToHeaders(metadata)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Http Status: $httpStatus, Connect Error Code: $connectCode")
      logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
      logger.trace(s"<<< Error processing request", e)
    }

    Response[F](httpStatus, headers = headers)
      .withMessage(
        connectrpc.Error(
          code = connectCode,
          message = message,
          details = details,
        )
      )
      .pure[F]
  }
}

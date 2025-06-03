package org.ivovk.connect_rpc_scala.connect

import io.grpc
import org.http4s
import org.ivovk.connect_rpc_scala.connect.StatusCodeMappings.*
import org.ivovk.connect_rpc_scala.grpc.{GrpcHeaders, StatusExceptionWithHeaders}

import scala.jdk.CollectionConverters.*

object ErrorHandling {

  case class ErrorDetails(
    httpStatusCode: Int,
    error: connectrpc.Error,
    headers: grpc.Metadata,
    trailers: grpc.Metadata,
  )

  def extractDetails(e: Throwable): ErrorDetails = {
    val grpcStatus = e match {
      case e: grpc.StatusException =>
        e.getStatus.getDescription match {
          case "an implementation is missing" =>
            grpc.Status.UNIMPLEMENTED
          case _ =>
            e.getStatus
        }
      case e: grpc.StatusRuntimeException =>
        e.getStatus
      case _: http4s.MessageFailure =>
        grpc.Status.INVALID_ARGUMENT
      case _ =>
        grpc.Status.INTERNAL
    }

    val (message, headers, trailers) = e match {
      case e: StatusExceptionWithHeaders =>
        (Option(e.getStatus.getDescription), e.getHeaders, e.getTrailers)
      case e: grpc.StatusException =>
        (Option(e.getStatus.getDescription), new grpc.Metadata(), e.getTrailers)
      case e: grpc.StatusRuntimeException =>
        (Option(e.getStatus.getDescription), new grpc.Metadata(), e.getTrailers)
      case e =>
        (Option(e.getMessage), new grpc.Metadata(), new grpc.Metadata())
    }

    val details = Option(trailers.removeAll(GrpcHeaders.ErrorDetailsKey))
      .fold(Seq.empty)(_.asScala.toSeq)

    ErrorDetails(
      httpStatusCode = grpcStatus.toHttpStatusCode,
      error = connectrpc.Error(
        code = grpcStatus.toConnectCode,
        message = message,
        details = details,
      ),
      headers = headers,
      trailers = trailers,
    )
  }

}

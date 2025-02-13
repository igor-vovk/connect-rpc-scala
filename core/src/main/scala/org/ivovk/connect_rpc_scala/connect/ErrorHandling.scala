package org.ivovk.connect_rpc_scala.connect

import io.grpc
import org.http4s
import org.ivovk.connect_rpc_scala.connect.StatusCodeMappings.*
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders

import scala.jdk.CollectionConverters.*

object ErrorHandling {

  case class ErrorDetails(
    httpStatusCode: Int,
    metadata: grpc.Metadata,
    error: connectrpc.Error,
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

    val (message, metadata) = e match {
      case e: grpc.StatusRuntimeException =>
        (Option(e.getStatus.getDescription), e.getTrailers)
      case e: grpc.StatusException =>
        (Option(e.getStatus.getDescription), e.getTrailers)
      case e =>
        (Option(e.getMessage), new grpc.Metadata())
    }

    val details = Option(metadata.removeAll(GrpcHeaders.ErrorDetailsKey))
      .fold(Seq.empty)(_.asScala.toSeq)

    ErrorDetails(
      httpStatusCode = grpcStatus.toHttpStatusCode,
      metadata = metadata,
      error = connectrpc.Error(
        code = grpcStatus.toConnectCode,
        message = message,
        details = details,
      ),
    )
  }

}

package org.ivovk.connect_rpc_scala

import io.grpc.{Metadata, Status}
import org.http4s.{Header, Headers}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders.asciiKey
import org.typelevel.ci.CIString

object Mappings extends HeaderMappings, StatusCodeMappings

trait HeaderMappings {

  extension (headers: Headers) {
    def toMetadata: Metadata = {
      val metadata = new Metadata()
      headers.foreach { header =>
        metadata.put(asciiKey(header.name.toString), header.value)
      }
      metadata
    }
  }

  extension (metadata: Metadata) {
    private def headers(prefix: String = ""): Headers = {
      val keys = metadata.keys()
      if (keys.isEmpty) return Headers.empty

      val b = List.newBuilder[Header.Raw]

      keys.forEach { key =>
        val name = CIString(prefix + key)

        metadata.getAll(asciiKey(key)).forEach { value =>
          b += Header.Raw(name, value)
        }
      }

      new Headers(b.result())
    }

    def toHeaders(trailing: Boolean = false): Headers = {
      val prefix = if trailing then "trailer-" else ""

      headers(prefix)
    }

  }

}

trait StatusCodeMappings {

  private val httpStatusCodesByGrpcStatusCode: Array[org.http4s.Status] = {
    val maxCode = io.grpc.Status.Code.values().map(_.value()).max
    val codes   = new Array[org.http4s.Status](maxCode + 1)

    io.grpc.Status.Code.values().foreach { code =>
      codes(code.value()) = code match {
        case io.grpc.Status.Code.CANCELLED =>
          org.http4s.Status.fromInt(499).getOrElse(sys.error("Should not happen"))
        case io.grpc.Status.Code.UNKNOWN => org.http4s.Status.InternalServerError
        case io.grpc.Status.Code.INVALID_ARGUMENT => org.http4s.Status.BadRequest
        case io.grpc.Status.Code.DEADLINE_EXCEEDED => org.http4s.Status.GatewayTimeout
        case io.grpc.Status.Code.NOT_FOUND => org.http4s.Status.NotFound
        case io.grpc.Status.Code.ALREADY_EXISTS => org.http4s.Status.Conflict
        case io.grpc.Status.Code.PERMISSION_DENIED => org.http4s.Status.Forbidden
        case io.grpc.Status.Code.RESOURCE_EXHAUSTED => org.http4s.Status.TooManyRequests
        case io.grpc.Status.Code.FAILED_PRECONDITION => org.http4s.Status.BadRequest
        case io.grpc.Status.Code.ABORTED => org.http4s.Status.Conflict
        case io.grpc.Status.Code.OUT_OF_RANGE => org.http4s.Status.BadRequest
        case io.grpc.Status.Code.UNIMPLEMENTED => org.http4s.Status.NotImplemented
        case io.grpc.Status.Code.INTERNAL => org.http4s.Status.InternalServerError
        case io.grpc.Status.Code.UNAVAILABLE => org.http4s.Status.ServiceUnavailable
        case io.grpc.Status.Code.DATA_LOSS => org.http4s.Status.InternalServerError
        case io.grpc.Status.Code.UNAUTHENTICATED => org.http4s.Status.Unauthorized
        case _ => org.http4s.Status.InternalServerError
      }
    }

    codes
  }

  private val connectErrorCodeByGrpcStatusCode: Array[connectrpc.Code] = {
    val maxCode = io.grpc.Status.Code.values().map(_.value()).max
    val codes   = new Array[connectrpc.Code](maxCode + 1)

    io.grpc.Status.Code.values().foreach { code =>
      codes(code.value()) = code match {
        case io.grpc.Status.Code.CANCELLED => connectrpc.Code.Canceled
        case io.grpc.Status.Code.UNKNOWN => connectrpc.Code.Unknown
        case io.grpc.Status.Code.INVALID_ARGUMENT => connectrpc.Code.InvalidArgument
        case io.grpc.Status.Code.DEADLINE_EXCEEDED => connectrpc.Code.DeadlineExceeded
        case io.grpc.Status.Code.NOT_FOUND => connectrpc.Code.NotFound
        case io.grpc.Status.Code.ALREADY_EXISTS => connectrpc.Code.AlreadyExists
        case io.grpc.Status.Code.PERMISSION_DENIED => connectrpc.Code.PermissionDenied
        case io.grpc.Status.Code.RESOURCE_EXHAUSTED => connectrpc.Code.ResourceExhausted
        case io.grpc.Status.Code.FAILED_PRECONDITION => connectrpc.Code.FailedPrecondition
        case io.grpc.Status.Code.ABORTED => connectrpc.Code.Aborted
        case io.grpc.Status.Code.OUT_OF_RANGE => connectrpc.Code.OutOfRange
        case io.grpc.Status.Code.UNIMPLEMENTED => connectrpc.Code.Unimplemented
        case io.grpc.Status.Code.INTERNAL => connectrpc.Code.Internal
        case io.grpc.Status.Code.UNAVAILABLE => connectrpc.Code.Unavailable
        case io.grpc.Status.Code.DATA_LOSS => connectrpc.Code.DataLoss
        case io.grpc.Status.Code.UNAUTHENTICATED => connectrpc.Code.Unauthenticated
        case _ => connectrpc.Code.Internal
      }
    }

    codes
  }

  extension (status: io.grpc.Status) {
    def toHttpStatus: org.http4s.Status = status.getCode.toHttpStatus
    def toConnectCode: connectrpc.Code = status.getCode.toConnectCode
  }

  // Url: https://connectrpc.com/docs/protocol/#error-codes
  extension (code: io.grpc.Status.Code) {
    def toHttpStatus: org.http4s.Status = httpStatusCodesByGrpcStatusCode(code.value())
    def toConnectCode: connectrpc.Code = connectErrorCodeByGrpcStatusCode(code.value())
  }

}

package org.ivovk.connect_rpc_scala

import io.grpc.{Metadata, Status}
import org.http4s.{Header, Headers, Response}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.syntax.all.{*, given}
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

import scala.collection.mutable

class HeaderMapping(
  headersFilter: String => Boolean,
  metadataFilter: String => Boolean,
  treatTrailersAsHeaders: Boolean,
) {

  private val keyCache: mutable.Map[String, Metadata.Key[String]] =
    new mutable.WeakHashMap[String, Metadata.Key[String]]()

  private inline def cachedAsciiKey(name: String): Metadata.Key[String] =
    keyCache.getOrElseUpdate(name, asciiKey(name))

  def toMetadata(headers: Headers): Metadata = {
    val metadata = new Metadata()
    headers.headers.foreach { header =>
      val headerName = header.name.toString
      if (headersFilter(headerName)) {
        metadata.put(metadataKeyByHeaderName(headerName), header.value)
      }
    }
    metadata
  }

  private def metadataKeyByHeaderName(name: String): Metadata.Key[String] =
    name match {
      case "User-Agent" | "user-agent" =>
        // Rename `User-Agent` to `x-user-agent` because `user-agent` gets overridden by gRPC
        GrpcHeaders.XUserAgentKey
      case _ =>
        cachedAsciiKey(name)
    }

  private def headers(
    metadata: Metadata,
    trailing: Boolean = false,
  ): Headers = {
    val keys = metadata.keys()
    if (keys.isEmpty) return Headers.empty

    val b = List.newBuilder[Header.Raw]

    keys.forEach { key =>
      if (metadataFilter(key)) {
        val name = if (trailing) CIString(s"trailer-$key") else CIString(key)

        metadata.getAll(cachedAsciiKey(key)).forEach { value =>
          b += Header.Raw(name, value)
        }
      }
    }

    new Headers(b.result())
  }

  def toHeaders(metadata: Metadata): Headers =
    headers(metadata)

  def trailersToHeaders(metadata: Metadata): Headers =
    headers(metadata, trailing = !treatTrailersAsHeaders)

}

object Mappings extends StatusCodeMappings, ResponseCodeExtensions

trait ResponseCodeExtensions {
  extension [F[_]](response: Response[F]) {
    def withMessage(entity: GeneratedMessage)(using codec: MessageCodec[F], options: EncodeOptions): Response[F] =
      codec.encode(entity, options).applyTo(response)
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

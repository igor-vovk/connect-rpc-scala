package org.ivovk.connect_rpc_scala

import io.grpc.{Metadata, Status}
import org.http4s.{Header, Headers}
import org.typelevel.ci.CIString
import scalapb.GeneratedMessage

object Mappings extends HeaderMappings, StatusCodeMappings, ProtoMappings

trait HeaderMappings {

  private inline def asciiKey(name: String): Metadata.Key[String] =
    Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER)

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

  extension (status: io.grpc.Status) {
    def toHttpStatus: org.http4s.Status = status.getCode.toHttpStatus
    def toConnectCode: String = status.getCode.toConnectCode
  }

  // Url: https://connectrpc.com/docs/protocol/#error-codes
  extension (code: io.grpc.Status.Code) {
    def toHttpStatus: org.http4s.Status = code match {
      case io.grpc.Status.Code.CANCELLED =>
        org.http4s.Status.fromInt(499).getOrElse(org.http4s.Status.InternalServerError)
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

    def toConnectCode: String = code match {
      case io.grpc.Status.Code.CANCELLED => "canceled"
      case io.grpc.Status.Code.UNKNOWN => "unknown"
      case io.grpc.Status.Code.INVALID_ARGUMENT => "invalid_argument"
      case io.grpc.Status.Code.DEADLINE_EXCEEDED => "deadline_exceeded"
      case io.grpc.Status.Code.NOT_FOUND => "not_found"
      case io.grpc.Status.Code.ALREADY_EXISTS => "already_exists"
      case io.grpc.Status.Code.PERMISSION_DENIED => "permission_denied"
      case io.grpc.Status.Code.RESOURCE_EXHAUSTED => "resource_exhausted"
      case io.grpc.Status.Code.FAILED_PRECONDITION => "failed_precondition"
      case io.grpc.Status.Code.ABORTED => "aborted"
      case io.grpc.Status.Code.OUT_OF_RANGE => "out_of_range"
      case io.grpc.Status.Code.UNIMPLEMENTED => "unimplemented"
      case io.grpc.Status.Code.INTERNAL => "internal"
      case io.grpc.Status.Code.UNAVAILABLE => "unavailable"
      case io.grpc.Status.Code.DATA_LOSS => "data_loss"
      case io.grpc.Status.Code.UNAUTHENTICATED => "unauthenticated"
      case _ => "internal"
    }
  }

}

trait ProtoMappings {

  extension [T <: GeneratedMessage](t: T) {
    private def any(typeUrlPrefix: String = "type.googleapis.com/"): com.google.protobuf.any.Any =
      com.google.protobuf.any.Any(
        typeUrl = typeUrlPrefix + t.companion.scalaDescriptor.fullName,
        value = t.toByteString
      )

    def toProtoAny: com.google.protobuf.any.Any = any()

    def toProtoErrorAny: com.google.protobuf.any.Any = any("")

  }

}
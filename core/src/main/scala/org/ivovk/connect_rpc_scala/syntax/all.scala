package org.ivovk.connect_rpc_scala.syntax

import io.grpc.{StatusException, StatusRuntimeException}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import scalapb.GeneratedMessage

object all extends RuntimeExceptionSyntax, ProtoMappingsSyntax

trait RuntimeExceptionSyntax {

  extension (e: StatusRuntimeException) {
    def withDetails[T <: GeneratedMessage](t: T): StatusRuntimeException = {
      e.getTrailers.put(GrpcHeaders.ErrorDetailsKey, connectrpc.ErrorDetailsAny(
        `type` = t.companion.scalaDescriptor.fullName,
        value = t.toByteString
      ))
      e
    }
  }

  extension (e: StatusException) {
    def withDetails[T <: GeneratedMessage](t: T): StatusException = {
      e.getTrailers.put(GrpcHeaders.ErrorDetailsKey, connectrpc.ErrorDetailsAny(
        `type` = t.companion.scalaDescriptor.fullName,
        value = t.toByteString
      ))
      e
    }
  }

}

trait ProtoMappingsSyntax {

  extension [T <: GeneratedMessage](t: T) {
    def toProtoAny: com.google.protobuf.any.Any = {
      com.google.protobuf.any.Any(
        typeUrl = "type.googleapis.com/" + t.companion.scalaDescriptor.fullName,
        value = t.toByteString
      )
    }
  }

}
package org.ivovk.connect_rpc_scala.syntax

import io.grpc.{StatusException, StatusRuntimeException}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import scalapb.GeneratedMessage

object all extends ExceptionSyntax, MetadataSyntax

trait ExceptionSyntax {

  extension (e: StatusRuntimeException) {
    def withDetails[T <: GeneratedMessage](t: T): StatusRuntimeException = {
      e.getTrailers.put(
        GrpcHeaders.ErrorDetailsKey,
        connectrpc.ErrorDetailsAny(
          `type` = t.companion.scalaDescriptor.fullName,
          value = t.toByteString,
        ),
      )
      e
    }
  }

  extension (e: StatusException) {
    def withDetails[T <: GeneratedMessage](t: T): StatusException = {
      e.getTrailers.put(
        GrpcHeaders.ErrorDetailsKey,
        connectrpc.ErrorDetailsAny(
          `type` = t.companion.scalaDescriptor.fullName,
          value = t.toByteString,
        ),
      )
      e
    }
  }

}

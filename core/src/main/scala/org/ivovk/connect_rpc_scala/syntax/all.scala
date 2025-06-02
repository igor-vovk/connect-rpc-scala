package org.ivovk.connect_rpc_scala.syntax

import io.grpc.{Metadata, StatusException, StatusRuntimeException}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import scalapb.GeneratedMessage

object all extends ExceptionSyntax, MetadataSyntax

trait ExceptionSyntax {

  extension (e: StatusRuntimeException) {
    def withDetails[T <: GeneratedMessage](t: T): StatusRuntimeException = {
      packDetails(e.getTrailers, t)
      e
    }
  }

  extension (e: StatusException) {
    def withDetails[T <: GeneratedMessage](t: T): StatusException = {
      packDetails(e.getTrailers, t);
      e
    }
  }

  def packDetails[T <: GeneratedMessage](metadata: Metadata, details: T): Unit =
    metadata.put(
      GrpcHeaders.ErrorDetailsKey,
      connectrpc.ErrorDetailsAny(
        `type` = details.companion.scalaDescriptor.fullName,
        value = details.toByteString,
      ),
    )

}

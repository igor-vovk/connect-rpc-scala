package org.ivovk.connect_rpc_scala.syntax

import com.google.protobuf.ByteString
import io.grpc.{StatusException, StatusRuntimeException}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

object all extends ExceptionSyntax, ProtoMappingsSyntax

trait ExceptionSyntax {

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
    def concat(other: T, more: T*): T = {
      val cmp   = t.companion.asInstanceOf[GeneratedMessageCompanion[T]]
      val empty = cmp.defaultInstance

      val els = (t :: other :: more.toList).filter(_ != empty)

      els match
        case Nil => empty
        case el :: Nil => el
        case _ =>
          val is = els.foldLeft(ByteString.empty)(_ concat _.toByteString).newCodedInput()
          cmp.parseFrom(is)
    }

    def toProtoAny: com.google.protobuf.any.Any = {
      com.google.protobuf.any.Any(
        typeUrl = "type.googleapis.com/" + t.companion.scalaDescriptor.fullName,
        value = t.toByteString
      )
    }
  }

}
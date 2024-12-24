package org.ivovk.connect_rpc_scala.grpc

import connectrpc.ErrorDetailsAny
import io.grpc.Metadata.Key
import org.ivovk.connect_rpc_scala.syntax.metadata
import org.ivovk.connect_rpc_scala.syntax.metadata.{*, given}

object GrpcHeaders {

  @deprecated("Use `asciiKey` from the `org.ivovk.connect_rpc_scala.syntax.metadata` package instead", "0.2.5")
  def asciiKey(name: String): Key[String] = metadata.asciiKey(name)

  val XUserAgentKey: Key[String] = metadata.asciiKey("x-user-agent")

  private[connect_rpc_scala] val ErrorDetailsKey: Key[ErrorDetailsAny] =
    binaryKey("connect-error-details-bin")(using binaryMarshaller(ErrorDetailsAny.parseFrom)(_.toByteArray))

}

package org.ivovk.connect_rpc_scala.grpc

import connectrpc.ErrorDetailsAny
import io.grpc.Metadata

object GrpcHeaders {

  val ErrorDetailsKey: Metadata.Key[ErrorDetailsAny] =
    Metadata.Key.of("connect-error-details-bin", new Metadata.BinaryMarshaller[ErrorDetailsAny] {
      override def toBytes(value: ErrorDetailsAny): Array[Byte] = value.toByteArray

      override def parseBytes(serialized: Array[Byte]): ErrorDetailsAny = ErrorDetailsAny.parseFrom(serialized)
    })

  val XUserAgentKey: Metadata.Key[String] = asciiKey("x-user-agent")

  inline def asciiKey(name: String): Metadata.Key[String] =
    Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER)

}

package org.ivovk.connect_rpc_scala.conformance

import connectrpc.conformance.v1.*
import org.ivovk.connect_rpc_scala.service.*

object ConformanceServiceDefinition {
  val ServiceName = "connectrpc.conformance.v1.ConformanceService"

  val Unary = UnaryMethodDescriptor(
    ServiceName,
    "Unary",
    UnaryRequest,
    UnaryResponse,
  )

  val IdempotentUnary = UnaryMethodDescriptor(
    ServiceName,
    "IdempotentUnary",
    IdempotentUnaryRequest,
    IdempotentUnaryResponse,
  )

  val Unimplemented = UnaryMethodDescriptor(
    ServiceName,
    "Unimplemented",
    UnimplementedRequest,
    UnimplementedResponse,
  )

  def bind[F[_]](service: ConformanceServiceImpl[F]): ServiceDefinition[F] =
    ServiceDefinition(
      ServiceName,
      Seq(
        UnaryMethod(Unary, service.unary),
        UnaryMethod(IdempotentUnary, service.idempotentUnary),
        UnaryMethod(Unimplemented, service.unimplemented),
      ),
    )
}

package me.ivovk.connect_rpc_scala

import scalapb.GeneratedMessage

object UnaryMethod {

  def apply[
    F[_],
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    methodDescriptor: UnaryMethodDescriptor[Request, Response],
    handler: (Request, ConnectRequestContext) => F[ConnectResponse[Response]],
  ): UnaryMethod[F] =
    new UnaryMethod[F] {
      override type RequestMessage  = Request
      override type ResponseMessage = Response

      override val descriptor: UnaryMethodDescriptor[Request, Response] = methodDescriptor

      override def invoke(
        request: Request,
        ctx: ConnectRequestContext,
      ): F[ConnectResponse[Response]] =
        handler(request, ctx)
    }
}

sealed trait UnaryMethod[F[_]] {
  type RequestMessage <: GeneratedMessage
  type ResponseMessage <: GeneratedMessage

  def descriptor: UnaryMethodDescriptor[RequestMessage, ResponseMessage]

  def invoke(
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): F[ConnectResponse[ResponseMessage]]
}

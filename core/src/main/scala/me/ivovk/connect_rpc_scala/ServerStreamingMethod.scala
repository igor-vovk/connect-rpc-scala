package me.ivovk.connect_rpc_scala

import cats.effect.Resource
import scalapb.GeneratedMessage

object ServerStreamingMethod {

  def apply[
    F[_],
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    methodDescriptor: ServerStreamingMethodDescriptor[Request, Response],
    handler: (Request, ConnectRequestContext) => Resource[F, ConnectStreamingResponse[F, Response]],
  ): ServerStreamingMethod[F] =
    new ServerStreamingMethod[F] {
      override type RequestMessage  = Request
      override type ResponseMessage = Response

      override val descriptor: ServerStreamingMethodDescriptor[Request, Response] = methodDescriptor

      override def invoke(
        request: Request,
        ctx: ConnectRequestContext,
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        handler(request, ctx)
    }
}

sealed trait ServerStreamingMethod[F[_]] {
  type RequestMessage <: GeneratedMessage
  type ResponseMessage <: GeneratedMessage

  def descriptor: ServerStreamingMethodDescriptor[RequestMessage, ResponseMessage]

  def invoke(
    request: RequestMessage,
    ctx: ConnectRequestContext,
  ): Resource[F, ConnectStreamingResponse[F, ResponseMessage]]
}

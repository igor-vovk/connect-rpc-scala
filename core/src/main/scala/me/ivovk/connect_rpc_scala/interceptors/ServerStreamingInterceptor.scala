package me.ivovk.connect_rpc_scala.interceptors

import cats.effect.Resource
import me.ivovk.connect_rpc_scala.{ConnectStreamingResponse, ServerStreamingMethodDescriptor}
import scalapb.GeneratedMessage

trait ServerStreamingInterceptor[F[_], In, Out] { self =>

  def intercept[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: ServerStreamingMethodDescriptor[Request, Response],
    request: Request,
    context: In,
    next: Out => Resource[F, ConnectStreamingResponse[F, Response]],
  ): Resource[F, ConnectStreamingResponse[F, Response]]

  final def andThen[Next](
    interceptor: ServerStreamingInterceptor[F, Out, Next]
  ): ServerStreamingInterceptor[F, In, Next] =
    new ServerStreamingInterceptor[F, In, Next] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: ServerStreamingMethodDescriptor[Request, Response],
        request: Request,
        context: In,
        next: Next => Resource[F, ConnectStreamingResponse[F, Response]],
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        self.intercept(
          descriptor,
          request,
          context,
          transformed => interceptor.intercept(descriptor, request, transformed, next),
        )
    }
}

object ServerStreamingInterceptor {

  def identity[F[_], Context]: ServerStreamingInterceptor[F, Context, Context] =
    new ServerStreamingInterceptor[F, Context, Context] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: ServerStreamingMethodDescriptor[Request, Response],
        request: Request,
        context: Context,
        next: Context => Resource[F, ConnectStreamingResponse[F, Response]],
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        next(context)
    }

  def transformContext[F[_], In, Out](
    transform: In => F[Out]
  ): ServerStreamingInterceptor[F, In, Out] =
    new ServerStreamingInterceptor[F, In, Out] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: ServerStreamingMethodDescriptor[Request, Response],
        request: Request,
        context: In,
        next: Out => Resource[F, ConnectStreamingResponse[F, Response]],
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        Resource.eval(transform(context)).flatMap(next)
    }
}

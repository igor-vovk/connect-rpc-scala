package me.ivovk.connect_rpc_scala.interceptors

import cats.effect.Resource
import me.ivovk.connect_rpc_scala.{
  ConnectRequestContext,
  ConnectStreamingResponse,
  ServerStreamingMethodDescriptor,
}
import scalapb.GeneratedMessage

trait ServerStreamingInterceptor[F[_]] { self =>

  def intercept[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: ServerStreamingMethodDescriptor[Request, Response],
    request: Request,
    context: ConnectRequestContext,
    next: ConnectRequestContext => Resource[F, ConnectStreamingResponse[F, Response]],
  ): Resource[F, ConnectStreamingResponse[F, Response]]

  final def andThen(
    interceptor: ServerStreamingInterceptor[F]
  ): ServerStreamingInterceptor[F] =
    new ServerStreamingInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: ServerStreamingMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => Resource[F, ConnectStreamingResponse[F, Response]],
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

  def identity[F[_]]: ServerStreamingInterceptor[F] =
    new ServerStreamingInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: ServerStreamingMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => Resource[F, ConnectStreamingResponse[F, Response]],
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        next(context)
    }

  def transformContext[F[_]](
    transform: ConnectRequestContext => F[ConnectRequestContext]
  ): ServerStreamingInterceptor[F] =
    new ServerStreamingInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: ServerStreamingMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => Resource[F, ConnectStreamingResponse[F, Response]],
      ): Resource[F, ConnectStreamingResponse[F, Response]] =
        Resource.eval(transform(context)).flatMap(next)
    }
}

package me.ivovk.connect_rpc_scala.service

import cats.{FlatMap, MonadThrow}
import scalapb.GeneratedMessage

trait UnaryInterceptor[F[_], In, Out] { self =>

  def intercept[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[Request, Response],
    request: Request,
    context: In,
    next: Out => F[ConnectResponse[Response]],
  ): F[ConnectResponse[Response]]

  final def andThen[Next](
    interceptor: UnaryInterceptor[F, Out, Next]
  ): UnaryInterceptor[F, In, Next] =
    new UnaryInterceptor[F, In, Next] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: In,
        next: Next => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] =
        self.intercept(
          descriptor,
          request,
          context,
          transformed => interceptor.intercept(descriptor, request, transformed, next),
        )
    }
}

object UnaryInterceptor {

  def identity[F[_], Context]: UnaryInterceptor[F, Context, Context] =
    new UnaryInterceptor[F, Context, Context] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: Context,
        next: Context => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] =
        next(context)
    }

  def transformContext[F[_], In, Out](
    transform: In => F[Out]
  )(using F: FlatMap[F]): UnaryInterceptor[F, In, Out] =
    new UnaryInterceptor[F, In, Out] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: In,
        next: Out => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] =
        F.flatMap(transform(context))(next)
    }

  def tapError[F[_], Context](
    onError: Throwable => F[Unit]
  )(using F: MonadThrow[F]): UnaryInterceptor[F, Context, Context] =
    new UnaryInterceptor[F, Context, Context] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: Context,
        next: Context => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] =
        F.handleErrorWith(next(context)) { error =>
          F.flatMap(onError(error))(_ => F.raiseError(error))
        }
    }

}

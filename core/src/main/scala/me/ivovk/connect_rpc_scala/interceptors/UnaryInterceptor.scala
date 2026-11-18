package me.ivovk.connect_rpc_scala.interceptors

import cats.{FlatMap, MonadThrow}
import me.ivovk.connect_rpc_scala.service.{ConnectResponse, UnaryMethodDescriptor}
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

  /** Observes a failed call with its method descriptor before propagating the error. */
  def tapError[F[_], Context](
    onError: (UnaryMethodDescriptor[?, ?], Throwable) => F[Unit]
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
      ): F[ConnectResponse[Response]] = {
        val response = F.flatMap(F.unit)(_ => F.flatten(F.catchNonFatal(next(context))))
        F.handleErrorWith(response) { error =>
          F.flatMap(onError(descriptor, error))(_ => F.raiseError(error))
        }
      }
    }

}

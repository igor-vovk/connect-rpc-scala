package me.ivovk.connect_rpc_scala.interceptors

import cats.{FlatMap, MonadThrow}
import me.ivovk.connect_rpc_scala.{ConnectRequestContext, ConnectResponse, UnaryMethodDescriptor}
import scalapb.GeneratedMessage

trait UnaryInterceptor[F[_]] { self =>

  def intercept[
    Request <: GeneratedMessage,
    Response <: GeneratedMessage,
  ](
    descriptor: UnaryMethodDescriptor[Request, Response],
    request: Request,
    context: ConnectRequestContext,
    next: ConnectRequestContext => F[ConnectResponse[Response]],
  ): F[ConnectResponse[Response]]

  final def andThen(
    interceptor: UnaryInterceptor[F]
  ): UnaryInterceptor[F] =
    new UnaryInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => F[ConnectResponse[Response]],
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

  /**
   * Composes a stack that handles requests in order and responses and errors in reverse order. An empty stack
   * forwards the call unchanged.
   */
  def chain[F[_]](interceptors: UnaryInterceptor[F]*): UnaryInterceptor[F] =
    interceptors.reduceRightOption(_.andThen(_)).getOrElse(identity[F])

  def identity[F[_]]: UnaryInterceptor[F] =
    new UnaryInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] =
        next(context)
    }

  def transformContext[F[_]](
    transform: ConnectRequestContext => F[ConnectRequestContext]
  )(using F: FlatMap[F]): UnaryInterceptor[F] =
    new UnaryInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] =
        F.flatMap(transform(context))(next)
    }

  /** Observes a failed call with its method descriptor before propagating the error. */
  def tapError[F[_]](
    onError: (UnaryMethodDescriptor[?, ?], Throwable) => F[Unit]
  )(using F: MonadThrow[F]): UnaryInterceptor[F] =
    new UnaryInterceptor[F] {
      override def intercept[
        Request <: GeneratedMessage,
        Response <: GeneratedMessage,
      ](
        descriptor: UnaryMethodDescriptor[Request, Response],
        request: Request,
        context: ConnectRequestContext,
        next: ConnectRequestContext => F[ConnectResponse[Response]],
      ): F[ConnectResponse[Response]] = {
        val response = F.flatMap(F.unit)(_ => F.flatten(F.catchNonFatal(next(context))))
        F.handleErrorWith(response) { error =>
          F.flatMap(onError(descriptor, error))(_ => F.raiseError(error))
        }
      }
    }

}

package org.ivovk.connect_rpc_scala.grpc

import cats.Applicative
import cats.effect.{Async, LiftIO}
import cats.syntax.all.*
import io.grpc.Metadata
import org.ivovk.connect_rpc_scala.util.FiberLocal

case class CallContext(
  responseHeaders: Option[Metadata],
  responseTrailers: Option[Metadata],
)

object CallContext {
  val empty = CallContext(None, None)
}

trait RequestContextHolder[F[_]] {

  def getResponseHeaders: F[Option[Metadata]]

  def setResponseHeaders(headers: Metadata): F[Unit]

  def addResponseHeader(key: Metadata.Key[String], value: String): F[Unit]

  def getResponseTrailers: F[Option[Metadata]]

  def setResponseTrailers(trailers: Metadata): F[Unit]

  def addResponseTrailer(key: Metadata.Key[String], value: String): F[Unit]

  def setHeadersAndTrailers(headers: Metadata, trailers: Metadata): F[Unit]

  def clear: F[Unit]

}

object RequestContextHolder {

  def noop[F[_]: Applicative]: RequestContextHolder[F] =
    new RequestContextHolder[F] {

      override def getResponseHeaders: F[Option[Metadata]] =
        Applicative[F].pure(None)

      override def setResponseHeaders(headers: Metadata): F[Unit] =
        Applicative[F].unit

      override def addResponseHeader(key: Metadata.Key[String], value: String): F[Unit] =
        Applicative[F].unit

      override def getResponseTrailers: F[Option[Metadata]] =
        Applicative[F].pure(None)

      override def setResponseTrailers(trailers: Metadata): F[Unit] =
        Applicative[F].unit

      override def addResponseTrailer(key: Metadata.Key[String], value: String): F[Unit] =
        Applicative[F].unit

      override def setHeadersAndTrailers(headers: Metadata, trailers: Metadata): F[Unit] =
        Applicative[F].unit

      override def clear: F[Unit] =
        Applicative[F].unit

    }

  def apply[F[_]: Async: LiftIO]: F[RequestContextHolder[F]] =
    FiberLocal[F, CallContext](CallContext.empty).map { fiberLocal =>
      new RequestContextHolder[F] {

        override def getResponseHeaders: F[Option[Metadata]] =
          fiberLocal.get.map(_.responseHeaders)

        override def setResponseHeaders(headers: Metadata): F[Unit] =
          fiberLocal.update(_.copy(responseHeaders = Some(headers)))

        override def addResponseHeader(key: Metadata.Key[String], value: String): F[Unit] =
          fiberLocal.update { ctx =>
            val headers = ctx.responseHeaders.getOrElse(new Metadata())
            headers.put(key, value)
            ctx.copy(responseHeaders = Some(headers))
          }

        override def getResponseTrailers: F[Option[Metadata]] =
          fiberLocal.get.map(_.responseTrailers)

        override def setResponseTrailers(trailers: Metadata): F[Unit] =
          fiberLocal.update(_.copy(responseTrailers = Some(trailers)))

        override def addResponseTrailer(key: Metadata.Key[String], value: String): F[Unit] =
          fiberLocal.update { ctx =>
            val trailers = ctx.responseTrailers.getOrElse(new Metadata())
            trailers.put(key, value)
            ctx.copy(responseTrailers = Some(trailers))
          }

        override def setHeadersAndTrailers(headers: Metadata, trailers: Metadata): F[Unit] =
          fiberLocal.update(_ => CallContext(Some(headers), Some(trailers)))

        override def clear: F[Unit] = fiberLocal.set(CallContext.empty)

      }
    }

}

package org.ivovk.connect_rpc_scala.util

import cats.Functor
import cats.effect.{IOLocal, LiftIO}
import cats.implicits.*

// Tagless-final abstraction for fiber-local storage.
trait FiberLocal[F[_], A] {

  def get: F[A]

  def set(a: A): F[Unit]

  def update(f: A => A): F[Unit]

}

object FiberLocal {

  def apply[F[_]: Functor, A](default: A)(using lft: LiftIO[F]): F[FiberLocal[F, A]] =
    lft
      .liftIO(IOLocal(default))
      .map(fromIOLocal)

  private def fromIOLocal[F[_], A](local: IOLocal[A])(using lft: LiftIO[F]): FiberLocal[F, A] =
    new FiberLocal[F, A] {
      override def get: F[A] =
        lft.liftIO(local.get)

      override def set(a: A): F[Unit] =
        lft.liftIO(local.set(a))

      override def update(f: A => A): F[Unit] =
        lft.liftIO(local.update(f))
    }

}

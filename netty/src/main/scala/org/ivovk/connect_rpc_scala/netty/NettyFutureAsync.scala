package org.ivovk.connect_rpc_scala.netty

import cats.effect.Async
import io.netty.util.concurrent.Future

val UnitRight: Either[Nothing, Unit]           = Right(())
val NoneLeft: Either[Option[Nothing], Nothing] = Left(None)

object NettyFutureAsync {
  def apply[F[_]: Async]: NettyFutureAsync[F] = new NettyFutureAsync[F] {}
}

trait NettyFutureAsync[F[_]: Async] {
  def fromFuture[A](future: => Future[A]): F[A] =
    Async[F].asyncCheckAttempt { cb =>
      Async[F].delay {
        val fut = future

        if fut.isDone && fut.isSuccess then Right(fut.getNow)
        else {
          fut.addListener { (f: Future[A]) =>
            if f.isSuccess then cb(Right(f.getNow))
            else cb(Left(f.cause()))
          }

          NoneLeft
        }
      }
    }

  def fromFuture_(future: => Future[?]): F[Unit] =
    Async[F].asyncCheckAttempt { cb =>
      Async[F].delay {
        val fut = future

        if fut.isDone && fut.isSuccess then UnitRight
        else {
          fut.addListener { (f: Future[?]) =>
            if f.isSuccess then cb(UnitRight)
            else cb(Left(f.cause()))
          }

          NoneLeft
        }
      }
    }

}

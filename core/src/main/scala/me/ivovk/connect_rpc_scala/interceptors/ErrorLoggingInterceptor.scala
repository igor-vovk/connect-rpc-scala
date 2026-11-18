package me.ivovk.connect_rpc_scala.interceptors

import cats.MonadThrow
import connectrpc.Code
import me.ivovk.connect_rpc_scala.http.ConnectHttpStatus
import me.ivovk.connect_rpc_scala.service.ConnectException
import org.slf4j.{Logger, LoggerFactory}

object ErrorLoggingInterceptor {

  opaque type ErrorLogger[F[_]] = (String, Throwable) => F[Unit]

  object ErrorLogger {

    def apply[F[_]](logger: (String, Throwable) => F[Unit]): ErrorLogger[F] = logger

    def fromSlf4j[F[_]](logger: => Logger)(using F: MonadThrow[F]): ErrorLogger[F] =
      (message, error) => F.catchNonFatal(logger.error(message, error))

    extension [F[_]](logger: ErrorLogger[F])
      def log(message: String, error: Throwable): F[Unit] = logger(message, error)
  }

  private lazy val defaultLogger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def builder[F[_]: MonadThrow]: Builder[F] =
    new Builder(
      ErrorLogger.fromSlf4j[F](defaultLogger),
      code => ConnectHttpStatus.fromCode(code) >= 500,
    )

  final class Builder[F[_]] private[ErrorLoggingInterceptor] (
    logger: ErrorLogger[F],
    shouldLog: Code => Boolean,
  )(using F: MonadThrow[F]) {

    private def copy(
      logger: ErrorLogger[F] = this.logger,
      shouldLog: Code => Boolean = this.shouldLog,
    ): Builder[F] =
      new Builder(logger, shouldLog)

    def withLogger(logger: (String, Throwable) => F[Unit]): Builder[F] =
      copy(logger = ErrorLogger(logger))

    def withLogger(logger: Logger): Builder[F] =
      copy(logger = ErrorLogger.fromSlf4j[F](logger))

    /** Replaces the default filter, which accepts codes that map to HTTP 5xx. */
    def withCodeFilter(shouldLog: Code => Boolean): Builder[F] =
      copy(shouldLog = shouldLog)

    def build[Context](): UnaryInterceptor[F, Context, Context] =
      UnaryInterceptor.tapError[F, Context] { (descriptor, error) =>
        val log = F.catchNonFatal {
          val code = error match {
            case exception: ConnectException => exception.code
            case _                           => Code.Internal
          }

          if shouldLog(code) then {
            logger(
              s"Connect service error: method=${descriptor.method} code=${code.name.stripPrefix("CODE_")}",
              error,
            )
          } else F.unit
        }

        F.handleError(F.flatten(log))(_ => ())
      }
  }
}

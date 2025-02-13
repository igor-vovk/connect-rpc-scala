package org.ivovk.connect_rpc_scala.http4s.connect

import cats.Applicative
import cats.implicits.*
import org.http4s.{Response, Status}
import org.ivovk.connect_rpc_scala.connect.ErrorHandling
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec
import org.ivovk.connect_rpc_scala.http4s.ResponseExtensions.*
import org.ivovk.connect_rpc_scala.http4s.{ErrorHandler, Http4sHeaderMapping}
import org.slf4j.LoggerFactory

class ConnectErrorHandler[F[_]: Applicative](
  headerMapping: Http4sHeaderMapping
) extends ErrorHandler[F] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def handle(e: Throwable)(using MessageCodec[F]): F[Response[F]] = {
    val details = ErrorHandling.extractDetails(e)
    val headers = headerMapping.trailersToHeaders(details.metadata)

    if (logger.isTraceEnabled) {
      logger.trace(
        s"<<< Http Status: ${details.httpStatusCode}, Connect Error Code: ${details.error.code}"
      )
      logger.trace(s"<<< Headers: ${headers.redactSensitive()}")
      logger.trace(s"<<< Error processing request", e)
    }

    val httpStatus = Status.fromInt(details.httpStatusCode).fold(throw _, identity)

    Response[F](httpStatus, headers = headers)
      .withMessage(details.error)
      .pure[F]
  }
}

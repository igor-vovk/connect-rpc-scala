package org.ivovk.connect_rpc_scala.netty.connect

import cats.effect.Async
import io.netty.handler.codec.http.*
import org.ivovk.connect_rpc_scala.connect.ErrorHandling
import org.ivovk.connect_rpc_scala.http.codec.{EncodeOptions, MessageCodec}
import org.ivovk.connect_rpc_scala.netty.headers.NettyHeaderMapping
import org.ivovk.connect_rpc_scala.netty.{ErrorHandler, Response}
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory

class ConnectErrorHandler[F[_]: Async](
  headerMapping: NettyHeaderMapping
) extends ErrorHandler[F] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def handle(e: Throwable)(using codec: MessageCodec[F]): F[HttpResponse] = {
    val details = ErrorHandling.extractDetails(e)
    val headers = headerMapping.trailersToHeaders(details.trailers)

    if (logger.isTraceEnabled) {
      logger.trace(
        s"<<< Http Status: ${details.httpStatusCode}, Connect Error Code: ${details.error.code}"
      )
      logger.trace(s"<<< Error processing request", e)
    }

    Response.create(
      message = details.error,
      status = HttpResponseStatus.valueOf(details.httpStatusCode),
      headers = headers,
    )
  }
}

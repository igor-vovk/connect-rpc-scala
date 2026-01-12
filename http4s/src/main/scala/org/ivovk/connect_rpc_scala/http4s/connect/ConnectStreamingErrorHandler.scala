package org.ivovk.connect_rpc_scala.http4s.connect

import cats.Applicative
import cats.implicits.*
import org.http4s.{Header, Headers, Response}
import org.ivovk.connect_rpc_scala.connect.ErrorHandling
import org.ivovk.connect_rpc_scala.http.HeaderMapping.cachedAsciiKey
import org.ivovk.connect_rpc_scala.http.MetadataToHeaders
import org.ivovk.connect_rpc_scala.http.codec.MessageCodec
import org.ivovk.connect_rpc_scala.http4s.ErrorHandler
import org.ivovk.connect_rpc_scala.http4s.ResponseBuilder.*
import org.slf4j.LoggerFactory
import org.typelevel.ci.CIString

import scala.jdk.CollectionConverters.*

class ConnectStreamingErrorHandler[F[_]: Applicative](
  headerMapping: MetadataToHeaders[Headers]
) extends ErrorHandler[F] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def handle(e: Throwable)(using MessageCodec[F]): F[Response[F]] = {
    val details = ErrorHandling.extractDetails(e)
    val headers = headerMapping.toHeaders(details.headers)

    val responseMetadata = Seq.newBuilder[connectrpc.MetadataEntry]
    val headersToAdd     = Seq.newBuilder[Header.Raw]

    details.trailers.keys.forEach { key =>
      if key.startsWith("trailer-") then
        responseMetadata += connectrpc.MetadataEntry(
          key = key.substring("trailer-".length),
          value = details.trailers.getAll(cachedAsciiKey(key)).asScala.toSeq,
        )
      else
        headersToAdd += Header.Raw(
          CIString(key),
          details.trailers.getAll(cachedAsciiKey(key)).asScala.mkString(","),
        )
    }

    if (logger.isTraceEnabled) {
      logger.trace(
        s"<<< Http Status: ${details.httpStatusCode}, Connect Error Code: ${details.error.code}"
      )
      logger.trace(s"<<< Error processing request", e)
    }

    mkStreamingResponse[F](
      headers ++ Headers(headersToAdd.result()),
      fs2.Stream(
        connectrpc.EndStreamMessage(
          error = Some(details.error),
          metadata = responseMetadata.result(),
        )
      ),
    ).pure[F]
  }
}

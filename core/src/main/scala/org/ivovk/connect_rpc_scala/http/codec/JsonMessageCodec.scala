package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import cats.implicits.*
import fs2.text.decodeWithCharset
import fs2.{Chunk, Stream}
import org.http4s.{DecodeResult, InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity, ResponseEntity}
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory
import scalapb.json4s.{Parser, Printer}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.net.URLDecoder

class JsonMessageCodec[F[_]: Sync](
  parser: Parser,
  printer: Printer,
  decodingTransform: JsonTransform = AsIsJsonTransform,
) extends MessageCodec[F] {

  private val logger     = LoggerFactory.getLogger(getClass)
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/json`

  override def decode[A <: Message](entity: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A] = {
    val charset = entity.charset
    val string = entity.message match {
      case str: String =>
        Sync[F].delay(URLDecoder.decode(str, charset))
      case stream: Stream[F, Byte] =>
        compressor
          .decompressed(entity.encoding, stream)
          .through(decodeWithCharset(charset))
          .compile
          .string
    }

    string
      .flatMap { str =>
        if (logger.isTraceEnabled) {
          logger.trace(
            s">>> Headers: ${GrpcHeaders.redactSensitiveHeaders(entity.headers)}"
          )
          logger.trace(s">>> JSON: $str")
        }

        if str.nonEmpty then
          Sync[F].delay {
            val json = JsonMethods.parse(str)

            parser.fromJson(decodingTransform(json))
          }
        else cmp.defaultInstance.pure[F]
      }
      .attemptT
      .leftMap(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](message: A, options: EncodeOptions): ResponseEntity[F] = {
    val string = printer.print(message)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< JSON: $string")
    }

    val bytes = string.getBytes()

    val entity = ResponseEntity[F](
      headers = Map("Content-Type" -> mediaType.show),
      body = Stream.chunk(Chunk.array(bytes)),
      length = Some(bytes.length.toLong),
    )

    compressor.compressed(options.encoding, entity)
  }

  def withDecodingJsonTransform(transform: JsonTransform): JsonMessageCodec[F] =
    if transform == this.decodingTransform then this
    else new JsonMessageCodec[F](parser, printer, transform)

}

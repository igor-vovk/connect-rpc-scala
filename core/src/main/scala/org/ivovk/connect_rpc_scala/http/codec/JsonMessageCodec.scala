package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import cats.implicits.*
import fs2.{Chunk, Stream}
import io.circe.Json
import io.circe.jawn.JawnParser
import io.circe.parser.parse
import org.http4s.{InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}
import scalapb_circe.{Parser, Printer}

import java.net.URLDecoder
import java.nio.charset.{Charset, StandardCharsets}
import scala.io.Source

private[codec] object CirceJsonParser {
  private val parser = new JawnParser()

  def parse(chunk: Chunk[Byte], charset: Charset): Json = {
    val result =
      if charset == StandardCharsets.UTF_8 then parser.parseByteBuffer(chunk.toByteBuffer)
      else parser.parseCharSequence(charset.decode(chunk.toByteBuffer))

    result.fold(throw _, identity)
  }
}

class JsonMessageCodec[F[_]: Sync](
  parser: Parser,
  printer: Printer,
  decodingTransform: JsonTransform = AsIsJsonTransform,
) extends MessageCodec[F] {

  private val logger     = LoggerFactory.getLogger(getClass)
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/json`

  override def decode[A <: Message](entity: EntityToDecode[F])(using cmp: Companion[A]): Stream[F, A] = {
    val stream = entity.message match {
      case str: String if str.isEmpty =>
        Stream.emit(cmp.defaultInstance)
      case s: String =>
        Stream.eval(Sync[F].delay {
          val str = URLDecoder.decode(s, entity.charset)

          if (logger.isTraceEnabled) {
            logger.trace(s">>> JSON: $str")
          }

          val json = parse(str).fold(throw _, identity)
          parser.fromJson(decodingTransform(json))
        })
      case stream: Stream[F, Byte] =>
        stream
          .through(compressor.decompress(entity.encoding))
          .chunkAll
          .evalMap { chunk =>
            if !chunk.isEmpty then
              Sync[F].delay {
                if (logger.isTraceEnabled) {
                  val str = entity.charset.decode(chunk.toByteBuffer).toString
                  logger.trace(s">>> JSON: $str")
                }

                val json = CirceJsonParser.parse(chunk, entity.charset)
                parser.fromJson(decodingTransform(json))
              }
            else Sync[F].pure(cmp.defaultInstance)
          }
    }

    stream.adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](message: Stream[F, A], options: EncodeOptions): EncodedEntity[F] = {
    val body = message
      .evalMap { m =>
        Sync[F].delay {
          val bytes = printer.toJson(m).noSpaces.getBytes(options.charset)

          if (logger.isTraceEnabled) {
            logger.trace(s"<<< JSON: ${Source.fromBytes(bytes, options.charset.name).mkString}")
          }

          Chunk.array(bytes)
        }
      }
      .flatMap(Stream.chunk)

    val entity = EncodedEntity[F](
      headers = Map(
        "Content-Type" -> mediaType.show
      ),
      body = body,
    )

    entity.pipe(compressor.compress(options.encoding))
  }

  def withDecodingJsonTransform(transform: JsonTransform): JsonMessageCodec[F] =
    if transform == this.decodingTransform then this
    else new JsonMessageCodec[F](parser, printer, transform)

}

package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import cats.implicits.*
import fs2.{Chunk, Stream}
import org.http4s.{InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory
import scalapb.json4s.{Parser, Printer}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.io.{InputStreamReader, StringReader}
import java.net.URLDecoder
import scala.io.Source

class JsonMessageCodec[F[_]: Sync](
  parser: Parser,
  printer: Printer,
  decodingTransform: JsonTransform = AsIsJsonTransform,
) extends MessageCodec[F] {

  private val logger     = LoggerFactory.getLogger(getClass)
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/json`

  private val jsonReader = JsonMethods.mapper.readerFor(classOf[JValue])

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

          val json = jsonReader.readValue[JValue](StringReader(str))
          parser.fromJson(decodingTransform(json))
        })
      case stream: Stream[F, Byte] =>
        stream
          .through(compressor.decompress(entity.encoding))
          .chunkAll
          .evalMap { chunk =>
            Sync[F].delay {
              val bv = chunk.toByteVector

              if (logger.isTraceEnabled) {
                val str = Source.fromInputStream(bv.toInputStream, entity.charset.name).mkString
                logger.trace(s">>> JSON: $str")
              }

              val json = jsonReader.readValue[JValue](InputStreamReader(bv.toInputStream, entity.charset))

              parser.fromJson(decodingTransform(json))
            }
          }
    }

    stream.adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](message: A, options: EncodeOptions): EncodedEntity[F] = {
    val json = printer.toJson(message)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< JSON: ${JsonMethods.compact(json)}")
    }

    val bytes = JsonMethods.mapper.writeValueAsBytes(json)

    val entity = EncodedEntity[F](
      headers = Map(
        "Content-Type" -> mediaType.show
      ),
      body = Stream.chunk(Chunk.array(bytes)),
    )

    entity.pipe(compressor.compress(options.encoding))
  }

  def withDecodingJsonTransform(transform: JsonTransform): JsonMessageCodec[F] =
    if transform == this.decodingTransform then this
    else new JsonMessageCodec[F](parser, printer, transform)

}

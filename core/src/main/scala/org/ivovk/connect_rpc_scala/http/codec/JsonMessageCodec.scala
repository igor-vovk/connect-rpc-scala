package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import cats.implicits.*
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import fs2.{Chunk, Stream}
import org.http4s.{InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory
import scalapb.json4s.{Parser, Printer}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.io.{ByteArrayOutputStream, InputStreamReader, OutputStreamWriter, StringReader}
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
              if (logger.isTraceEnabled) {
                val str = Source.fromBytes(chunk.toArray, entity.charset.name).mkString
                logger.trace(s">>> JSON: $str")
              }

              val reader = InputStreamReader(ByteBufferBackedInputStream(chunk.toByteBuffer), entity.charset)
              val json   = jsonReader.readValue[JValue](reader)

              parser.fromJson(decodingTransform(json))
            }
          }
    }

    stream.adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](message: Stream[F, A], options: EncodeOptions): EncodedEntity[F] = {
    val body = message
      .evalMap { m =>
        Sync[F].delay {
          val bytes = {
            val json   = printer.toJson(m)
            val baos   = ByteArrayOutputStream(128)
            val writer = OutputStreamWriter(baos, options.charset)
            JsonMethods.mapper.writeValue(writer, json)

            baos.toByteArray
          }

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

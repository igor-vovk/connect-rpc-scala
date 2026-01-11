package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import cats.implicits.*
import fs2.interop.scodec.StreamDecoder
import fs2.{Chunk, Stream}
import org.http4s.{InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory
import scalapb.json4s.{Parser, Printer}
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.io.InputStreamReader
import scala.io.Source

class JsonStreamingMessageCodec[F[_]: Sync](
  parser: Parser,
  printer: Printer,
) extends MessageCodec[F] {
  private val logger     = LoggerFactory.getLogger(getClass)
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/connect+json`

  private val jsonReader = JsonMethods.mapper.readerFor(classOf[JValue])

  override def decode[A <: Message](entity: EntityToDecode[F])(using cmp: Companion[A]): Stream[F, A] = {
    val stream = entity.message match {
      case sb: Stream[F, Byte] => sb
      case _                   =>
        Stream.raiseError[F](
          new UnsupportedOperationException(
            "Decoding JSON from query parameter is not supported in streaming codec"
          )
        )
    }

    stream
      .through(StreamDecoder.many(EnvelopedMessage.codec).toPipeByte)
      .flatMap { envelope =>
        Stream.chunk[F, Byte](Chunk.byteVector(envelope.data))
          .pipeIf(envelope.isCompressed) {
            _.through(compressor.decompress(entity.encoding))
          }
          .chunkAll
      }
      .evalMap { chunk =>
        if chunk.nonEmpty then
          Sync[F].delay {
            val bv   = chunk.toByteVector
            val json = jsonReader.readValue[JValue](InputStreamReader(bv.toInputStream, entity.charset))

            if (logger.isTraceEnabled) {
              val str = Source.fromInputStream(bv.toInputStream, entity.charset.name).mkString
              logger.trace(s">>> JSON: $str")
            }

            parser.fromJson(json)
          }
        else Sync[F].pure(cmp.defaultInstance)
      }
      .adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
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
}

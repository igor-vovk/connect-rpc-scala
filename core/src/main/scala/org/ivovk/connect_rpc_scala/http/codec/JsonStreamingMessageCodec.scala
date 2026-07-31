package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Sync
import cats.implicits.*
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.{Chunk, Stream}
import io.circe.Printer as CircePrinter
import org.http4s.{InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.MediaTypes
import org.ivovk.connect_rpc_scala.util.PipeSyntax.*
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}
import scalapb_circe.{Parser, Printer}
import scodec.bits.ByteVector

class JsonStreamingMessageCodec[F[_]: Sync](
  parser: Parser,
  printer: Printer,
) extends MessageCodec[F] {
  private val logger     = LoggerFactory.getLogger(getClass)
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/connect+json`

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
            if (logger.isTraceEnabled) {
              val str = entity.charset.decode(chunk.toByteBuffer).toString
              logger.trace(s">>> JSON: $str")
            }

            val json = CirceJsonParser.parse(chunk, entity.charset)
            parser.fromJson(json)
          }
        else Sync[F].pure(cmp.defaultInstance)
      }
      .adaptError(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](messages: Stream[F, A], options: EncodeOptions): EncodedEntity[F] = {
    val body = messages
      .evalMap { message =>
        Sync[F].delay {
          val bytes = CircePrinter.noSpaces.printToByteBuffer(printer.toJson(message), options.charset)

          if (logger.isTraceEnabled) {
            logger.trace(s"<<< JSON: ${options.charset.decode(bytes.asReadOnlyBuffer)}")
          }

          EnvelopedMessage(ByteVector.view(bytes))
            .withEndStream(message.isInstanceOf[connectrpc.EndStreamMessage])
        }
      }
      .through(StreamEncoder.many(EnvelopedMessage.codec).toPipeByte)

    EncodedEntity[F](
      headers = Map(
        "Content-Type" -> mediaType.show
      ),
      body = body,
    )
  }
}

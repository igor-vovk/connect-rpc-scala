package org.ivovk.connect_rpc_scala.http.codec

import cats.effect.Async
import cats.implicits.*
import com.google.protobuf.CodedOutputStream
import fs2.Stream
import fs2.io.{readOutputStream, toInputStreamResource}
import org.http4s.{DecodeResult, Entity, InvalidMessageBodyFailure, MediaType}
import org.ivovk.connect_rpc_scala.http.{MediaTypes, RequestEntity}
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import java.util.Base64
import scala.util.chaining.*

class ProtoMessageCodec[F[_] : Async] extends MessageCodec[F] {

  private val logger     = LoggerFactory.getLogger(getClass)
  private val base64dec  = Base64.getUrlDecoder
  private val compressor = Compressor[F]()

  override val mediaType: MediaType = MediaTypes.`application/proto`

  override def decode[A <: Message](entity: RequestEntity[F])(using cmp: Companion[A]): DecodeResult[F, A] = {
    val msg = entity.message match {
      case str: String =>
        Async[F].delay(cmp.parseFrom(base64dec.decode(str.getBytes(entity.charset.nioCharset))))
      case stream: Stream[F, Byte] =>
        toInputStreamResource(compressor.decompressed(entity.encoding, stream))
          .use(is => Async[F].delay(cmp.parseFrom(is)))
    }

    msg
      .pipe(
        if logger.isTraceEnabled then
          _.map { msg =>
            logger.trace(s">>> Headers: ${entity.headers.redactSensitive()}")
            logger.trace(s">>> Proto: ${msg.toProtoString}")
            msg
          }
        else identity
      )
      .attemptT
      .leftMap(e => InvalidMessageBodyFailure(e.getMessage, e.some))
  }

  override def encode[A <: Message](message: A, options: EncodeOptions): Entity[F] = {
    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Proto: ${message.toProtoString}")
    }

    val dataLength = message.serializedSize
    val chunkSize  = CodedOutputStream.DEFAULT_BUFFER_SIZE min dataLength

    val entity = Entity(
      body = readOutputStream(chunkSize)(os => Async[F].delay(message.writeTo(os))),
      length = Some(dataLength.toLong),
    )

    compressor.compressed(options.encoding, entity)
  }

}

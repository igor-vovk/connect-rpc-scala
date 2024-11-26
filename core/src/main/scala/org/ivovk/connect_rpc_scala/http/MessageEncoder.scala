package org.ivovk.connect_rpc_scala.http

import org.http4s.headers.`Content-Type`
import org.http4s.{Entity, EntityEncoder, MediaType}
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import scalapb.json4s.Printer

/**
 * `ResponseEncoder` difference from standard `EntityEncoder`: it has a message type in the method definition
 * type parameter, not requiring instantiation on each request.
 */
trait MessageEncoder[F[_]] {

  def encode[A <: GeneratedMessage](message: A): Entity[F]

  val mediaType: MediaType
}

object MessageEncoder {

  given [F[_], A <: GeneratedMessage](using encoder: MessageEncoder[F]): EntityEncoder[F, A] =
    EntityEncoder.encodeBy(`Content-Type`(encoder.mediaType))(encoder.encode)

}

class JsonMessageEncoder[F[_]](jsonPrinter: Printer) extends MessageEncoder[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def encode[A <: GeneratedMessage](message: A): Entity[F] = {
    val string = jsonPrinter.print(message)

    if (logger.isTraceEnabled) {
      logger.trace(s"<<< JSON: $string")
    }

    EntityEncoder.stringEncoder[F].toEntity(string)
  }

  override val mediaType: MediaType = MediaType.application.`json`

}

class ProtoMessageEncoder[F[_]] extends MessageEncoder[F] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def encode[A <: GeneratedMessage](message: A): Entity[F] = {
    if (logger.isTraceEnabled) {
      logger.trace(s"<<< Proto: ${message.toProtoString}")
    }

    Entity(
      body = fs2.Stream.emits(message.toByteArray).covary[F],
      length = Some(message.serializedSize.toLong),
    )
  }

  override val mediaType: MediaType = Entities.`application/proto`
}

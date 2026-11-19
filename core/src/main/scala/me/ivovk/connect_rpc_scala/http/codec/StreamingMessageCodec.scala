package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.Async
import cats.implicits.*
import fs2.{Chunk, Pull, Stream}
import io.circe.{Json, JsonObject, Printer as CircePrinter}
import io.circe.parser.parse
import me.ivovk.connect_rpc_scala.http.headers.`Connect-Content-Encoding`
import me.ivovk.connect_rpc_scala.http.json.ConnectErrorFormat
import me.ivovk.connect_rpc_scala.*
import org.http4s.headers.`Content-Type`
import org.http4s.{ContentCoding, Header, Headers, MediaType}
import org.typelevel.ci.*
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Locale

private object StreamingMessageCodec {
  private val EnvelopeHeaderSize = 5
  private val CompressedFlag     = 0x01
  private val EndStreamFlag      = 0x02
  private val KnownFlags         = CompressedFlag | EndStreamFlag
}

private[connect_rpc_scala] final class StreamingMessageCodec[F[_]: Async](
  messageCodec: MessageCodec[F],
  jsonSerdes: JsonSerdes[F],
  val mediaType: MediaType,
) {

  import StreamingMessageCodec.*

  private final case class Envelope(flags: Int, payload: Chunk[Byte]) {
    def compressed: Boolean = (flags & CompressedFlag) != 0
    def endStream: Boolean  = (flags & EndStreamFlag) != 0
  }

  private final case class EndStream(
    trailers: Headers,
    error: Option[ConnectException],
  )

  private val compressor = Compressor[F]()

  private val encodedEntityHeaders = Headers(`Content-Type`(mediaType))

  def encodeRequest[A <: GeneratedMessage](messages: Stream[F, A]): EncodedEntity[F] =
    EncodedEntity(
      headers = encodedEntityHeaders,
      body = messages.flatMap(encodeMessage),
    )

  def decodeRequest[A <: GeneratedMessage](
    body: Stream[F, Byte],
    headers: Headers,
  )(using companion: GeneratedMessageCompanion[A]): Stream[F, A] = {
    val coding = contentEncoding(headers)

    decodeEnvelopes(body).evalMap { envelope =>
      if envelope.endStream then
        ConnectException(
          connectrpc.Code.InvalidArgument,
          "Request stream contained an end-stream envelope",
        ).raiseError
      else decodeMessage[A](envelope, coding)
    }
  }

  def encodeResponse[A <: GeneratedMessage](
    response: ConnectStreamingResponse[F, A]
  ): EncodedEntity[F] = {
    val body = response.stream.events
      .flatMap {
        case ConnectStream.Event.Message(message) => encodeMessage(message)
        case ConnectStream.Event.End(trailers)    => encodeEndStream(trailers, None)
      }
      .handleErrorWith(error => encodeEndStream(errorTrailers(error), Some(asConnectException(error))))

    EncodedEntity(
      headers = encodedEntityHeaders,
      body = body,
    )
  }

  def decodeResponse[A <: GeneratedMessage](
    body: Stream[F, Byte],
    headers: Headers,
  )(using companion: GeneratedMessageCompanion[A]): ConnectStream[F, A] = {
    val coding = contentEncoding(headers)

    def go(envelopes: Stream[F, Envelope]): Pull[F, ConnectStream.Output[A], Headers] =
      envelopes.pull.uncons1.flatMap {
        case None =>
          Pull.raiseError(
            ConnectException(connectrpc.Code.Internal, "Response stream ended without an end-stream envelope")
          )
        case Some((envelope, tail)) if envelope.endStream =>
          Pull.eval(decodeEndStream(envelope, coding)).flatMap { end =>
            tail.pull.uncons1.flatMap {
              case Some(_) =>
                Pull.raiseError(
                  ConnectException(
                    connectrpc.Code.Internal,
                    "Response stream contained data after the end-stream envelope",
                  )
                )
              case None =>
                end.error match {
                  case Some(error) => Pull.raiseError(error.copy(headers = headers))
                  case None        => Pull.pure(end.trailers)
                }
            }
          }
        case Some((envelope, tail)) =>
          Pull.eval(decodeMessage[A](envelope, coding))
            .flatMap(message => ConnectStream.output1(message) >> go(tail))
      }

    ConnectStream.fromPull(go(decodeEnvelopes(body)))
  }

  private def encodeMessage(message: GeneratedMessage): Stream[F, Byte] =
    Stream.suspend(envelope(0, messageCodec.encodeChunk(message, StandardCharsets.UTF_8)))

  private def encodeEndStream(
    trailers: Headers,
    error: Option[ConnectException],
  ): Stream[F, Byte] = {
    val metadata = Json.fromJsonObject(
      JsonObject.fromIterable(
        trailers.headers.groupBy(_.name).iterator.map { (name, values) =>
          name.toString.toLowerCase(Locale.ROOT) -> Json.fromValues(
            values.map(header => Json.fromString(header.value))
          )
        }.toList
      )
    )
    val fields = List.concat(
      error.map { exception =>
        "error" -> ConnectErrorFormat.writer(
          jsonSerdes.printer,
          connectrpc.Error(
            code = exception.code,
            message = Option(exception.getMessage).filter(_.nonEmpty),
            details = exception.details,
          ),
        )
      },
      Option.when(trailers.headers.nonEmpty)("metadata" -> metadata),
    )
    val payload = Chunk.array(CircePrinter.noSpaces.print(Json.obj(fields*)).getBytes(StandardCharsets.UTF_8))

    envelope(EndStreamFlag, payload)
  }

  private def asConnectException(error: Throwable): ConnectException =
    error match {
      case connect: ConnectException => connect
      case other                     =>
        ConnectException(
          connectrpc.Code.Internal,
          Option(other.getMessage).getOrElse("Internal error"),
        ).withCause(other)
    }

  private def errorTrailers(error: Throwable): Headers =
    error match {
      case connect: ConnectException => connect.trailers
      case _                         => Headers.empty
    }

  private def decodeMessage[A <: GeneratedMessage](
    envelope: Envelope,
    coding: ContentCoding,
  )(using companion: GeneratedMessageCompanion[A]): F[A] =
    decodedPayload(envelope, coding).flatMap { payload =>
      messageCodec.decode[A](
        EntityToDecode(
          Stream.chunk(payload),
          Headers(`Content-Type`(messageCodec.mediaType)),
        )
      ).compile.onlyOrError
    }

  private def decodeEndStream(envelope: Envelope, coding: ContentCoding): F[EndStream] =
    decodedPayload(envelope, coding).flatMap { payload =>
      Async[F].delay {
        val json = parse(new String(payload.toArray, StandardCharsets.UTF_8)).fold(throw _, identity)
        val obj  = json.asObject.getOrElse(
          throw new IllegalArgumentException("End-stream payload must be a JSON object")
        )

        val trailers = obj("metadata").fold(Headers.empty)(decodeMetadata)
        val error    = obj("error").filterNot(_.isNull).map { errorJson =>
          val error = ConnectErrorFormat.parser(jsonSerdes.parser, errorJson)
          ConnectException(
            code = if error.code.isUnspecified then connectrpc.Code.Unknown else error.code,
            message = error.getMessage,
            details = error.details,
            trailers = trailers,
          )
        }

        EndStream(trailers, error)
      }.adaptError { case error =>
        ConnectException(
          connectrpc.Code.Internal,
          Option(error.getMessage).getOrElse("Invalid end-stream payload"),
        ).withCause(error)
      }
    }

  private def decodeMetadata(json: Json): Headers = {
    val obj =
      json.asObject.getOrElse(throw new IllegalArgumentException("End-stream metadata must be an object"))

    val headers = obj.toIterable.toList.flatMap { (name, value) =>
      val values = value.asArray.getOrElse(
        throw new IllegalArgumentException(s"End-stream metadata '$name' must be an array")
      )
      values.map { item =>
        Header.Raw(
          CIString(name),
          item.asString.getOrElse(
            throw new IllegalArgumentException(s"End-stream metadata '$name' must contain strings")
          ),
        )
      }
    }
    Headers(headers)
  }

  private def contentEncoding(headers: Headers): ContentCoding =
    headers.getWithWarnings[`Connect-Content-Encoding`].fold(ContentCoding.identity) { result =>
      result.toEither.fold(failures => throw failures.head, _.contentCoding)
    }

  private def decodedPayload(envelope: Envelope, coding: ContentCoding): F[Chunk[Byte]] =
    if !envelope.compressed then Async[F].pure(envelope.payload)
    else if coding == ContentCoding.identity then
      ConnectException(
        connectrpc.Code.Internal,
        "Compressed envelope used without connect-content-encoding",
      ).raiseError
    else
      Stream.chunk(envelope.payload)
        .through(compressor.decompress(coding))
        .compile
        .to(Array)
        .map(Chunk.array)

  private def envelope(flags: Int, payload: Chunk[Byte]): Stream[F, Byte] = {
    val header = ByteBuffer.allocate(EnvelopeHeaderSize)
      .put(flags.toByte)
      .putInt(payload.size)
      .array()

    Stream.chunk(Chunk.array(header)) ++ Stream.chunk(payload)
  }

  private def decodeEnvelopes(body: Stream[F, Byte]): Stream[F, Envelope] = {
    def go(remaining: Stream[F, Byte]): Pull[F, Envelope, Unit] =
      remaining.pull.unconsN(EnvelopeHeaderSize, allowFewer = true).flatMap {
        case None                                                  => Pull.done
        case Some((header, _)) if header.size < EnvelopeHeaderSize =>
          Pull.raiseError(
            ConnectException(connectrpc.Code.InvalidArgument, "Truncated streaming envelope header")
          )
        case Some((header, afterHeader)) =>
          val flags         = header(0) & 0xff
          val reservedFlags = flags & ~KnownFlags
          if reservedFlags != 0 then
            Pull.raiseError(
              ConnectException(
                connectrpc.Code.InvalidArgument,
                f"Streaming envelope used reserved flags 0x$reservedFlags%02x",
              )
            )
          else {
            val length = ByteBuffer.wrap(header.drop(1).toArray).getInt & 0xffffffffL
            if length > Int.MaxValue then
              Pull.raiseError(
                ConnectException(
                  connectrpc.Code.ResourceExhausted,
                  s"Streaming envelope is too large: $length bytes",
                )
              )
            else
              afterHeader.pull.unconsN(length.toInt, allowFewer = true).flatMap {
                case None if length == 0 => Pull.output1(Envelope(flags, Chunk.empty)) >> go(Stream.empty)
                case None                =>
                  Pull.raiseError(
                    ConnectException(connectrpc.Code.InvalidArgument, "Truncated streaming envelope payload")
                  )
                case Some((payload, _)) if payload.size != length.toInt =>
                  Pull.raiseError(
                    ConnectException(connectrpc.Code.InvalidArgument, "Truncated streaming envelope payload")
                  )
                case Some((payload, tail)) => Pull.output1(Envelope(flags, payload)) >> go(tail)
              }
          }
      }

    go(body).stream
  }
}

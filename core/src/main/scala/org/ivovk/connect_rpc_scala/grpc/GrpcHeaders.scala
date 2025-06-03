package org.ivovk.connect_rpc_scala.grpc

import connectrpc.ErrorDetailsAny
import io.grpc.Metadata
import io.grpc.Metadata.{AsciiMarshaller, Key}
import org.ivovk.connect_rpc_scala.syntax.metadata.{*, given}

import java.nio.charset.Charset

object GrpcHeaders {

  val XUserAgentKey: Key[String] = asciiKey("x-user-agent")

  private[connect_rpc_scala] val ErrorDetailsKey: Key[ErrorDetailsAny] =
    binaryKey("connect-error-details-bin")(using binaryMarshaller(ErrorDetailsAny.parseFrom)(_.toByteArray))

  case class ContentType(mediaType: String, charset: Option[String] = None) {
    def nioCharset: Option[Charset] = charset.map(Charset.forName)
  }

  given AsciiMarshaller[ContentType] = asciiMarshaller { s =>
    if s.contains(";") then
      val arr       = s.split("; charset=")
      val mediaType = arr(0)
      val charset   = if (arr.length > 1) Some(arr(1)) else None

      ContentType(mediaType, charset)
    else ContentType(s)
  }(c => c.charset.fold(c.mediaType)(charset => s"${c.mediaType}; charset=$charset"))

  private[connect_rpc_scala] val ContentTypeKey: Key[ContentType] = asciiKey("content-type")

  private[connect_rpc_scala] val ContentEncodingKey: Key[String] = asciiKey("content-encoding")

  private[connect_rpc_scala] val XTestCaseNameKey: Key[String] = asciiKey("x-test-case-name")

  private[connect_rpc_scala] val ConnectTimeoutMsKey: Key[Long] =
    asciiKey("connect-timeout-ms")(using asciiMarshaller(_.toLong)(_.toString))

  private[connect_rpc_scala] val CookieKey: Key[String] = asciiKey("cookie")

  private[connect_rpc_scala] val SetCookieKey: Key[String] = asciiKey("set-cookie")

  private[connect_rpc_scala] val AuthorizationKey: Key[String] = asciiKey("authorization")

  private val SensitiveHeaders: Set[Key[?]] = Set(AuthorizationKey, CookieKey, SetCookieKey)

  def redactSensitiveHeaders(
    headers: Metadata,
    headersToRemove: Set[Key[?]] = SensitiveHeaders,
  ): Metadata = {
    val headers2 = new Metadata()

    headers2.merge(headers)
    headersToRemove.foreach(headers2.discardAll)

    headers2
  }

  def splitIntoHeadersAndTrailers(
    metadata: Metadata
  ): (Metadata, Metadata) = {
    val headers  = new Metadata()
    val trailers = new Metadata()

    metadata.keys().forEach { name =>
      val key = asciiKey(name)

      if name.startsWith("trailer-") then
        val trailerKey = asciiKey(name.stripPrefix("trailer-"))

        metadata.getAll(key).forEach { value =>
          trailers.put(trailerKey, value)
        }
      else
        metadata.getAll(key).forEach { value =>
          headers.put(key, value)
        }
    }

    (headers, trailers)
  }

}

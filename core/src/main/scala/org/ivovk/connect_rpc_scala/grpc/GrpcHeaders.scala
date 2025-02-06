package org.ivovk.connect_rpc_scala.grpc

import connectrpc.ErrorDetailsAny
import io.grpc.Metadata
import io.grpc.Metadata.{AsciiMarshaller, Key}
import org.ivovk.connect_rpc_scala.syntax.metadata
import org.ivovk.connect_rpc_scala.syntax.metadata.{*, given}

import java.nio.charset.Charset
import scala.annotation.targetName
import scala.jdk.CollectionConverters.*

object GrpcHeaders {

  val XUserAgentKey: Key[String] = metadata.asciiKey("x-user-agent")

  private[connect_rpc_scala] val ErrorDetailsKey: Key[ErrorDetailsAny] =
    binaryKey("connect-error-details-bin")(using binaryMarshaller(ErrorDetailsAny.parseFrom)(_.toByteArray))

  case class ContentType(mediaType: String, charset: Option[String] = None) {
    def nioCharset: Option[Charset] = charset.map(Charset.forName)
  }

  given AsciiMarshaller[ContentType] = asciiMarshaller { s =>
    val Array(mediaType, charset) = s.split("; charset=")
    ContentType(mediaType, Option(charset))
  }(c => c.charset.fold(c.mediaType)(charset => s"${c.mediaType}; charset=$charset"))

  private[connect_rpc_scala] val ContentTypeKey: Key[ContentType] = metadata.asciiKey("content-type")

  private[connect_rpc_scala] val ContentEncodingKey: Key[String] = metadata.asciiKey("content-encoding")

  @targetName("XTestCaseName")
  case class `X-Test-Case-Name`(value: String)

  private[connect_rpc_scala] val XTestCaseNameKey: Key[`X-Test-Case-Name`] =
    metadata.asciiKey("x-test-case-name")(using asciiMarshaller(`X-Test-Case-Name`.apply)(_.value))

  @targetName("ConnectTimeoutMs")
  case class `Connect-Timeout-Ms`(value: Long)

  private[connect_rpc_scala] val ConnectTimeoutMsKey: Key[`Connect-Timeout-Ms`] =
    metadata.asciiKey("connect-timeout-ms")(
      using asciiMarshaller(s => `Connect-Timeout-Ms`(s.toLong))(_.value.toString)
    )

  private[connect_rpc_scala] val CookieKey: Key[String] = metadata.asciiKey("cookie")

  private[connect_rpc_scala] val SetCookieKey: Key[String] = metadata.asciiKey("set-cookie")

  private[connect_rpc_scala] val AuthorizationKey: Key[String] = metadata.asciiKey("authorization")

  def redactSensitiveHeaders(headers: Metadata): Metadata = {
    val headers2 = new Metadata()
    val keys     = headers.keys()

    for (keyName <- keys.iterator().asScala)
      if (
        keyName == AuthorizationKey.name() || keyName == CookieKey.name() || keyName == SetCookieKey.name()
      ) {
        headers2.put(asciiKey[String](keyName), "REDACTED")
      } else {
        val key = metadata.asciiKey(keyName)
        headers2.put(key, headers2.get(key))
      }

    headers2
  }

}

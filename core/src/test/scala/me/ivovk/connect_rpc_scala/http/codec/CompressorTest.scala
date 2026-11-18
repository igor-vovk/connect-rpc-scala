package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import connectrpc.{Code, Error}
import fs2.Stream
import org.http4s.headers.{`Content-Encoding`, `Content-Length`, `Content-Type`}
import org.http4s.{ContentCoding, Header, Headers, MediaType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

import java.nio.charset.StandardCharsets

class CompressorTest extends AnyFunSuite with Matchers {

  test("gzip updates encoding and removes the original content length") {
    val bytes  = List[Byte](1, 2, 3)
    val entity = EncodedEntity[IO](
      Headers(
        Header.Raw(ci"content-length", "3"),
        `Content-Encoding`(ContentCoding.identity),
        `Content-Type`(MediaType.application.json),
      ),
      Stream.emits(bytes).covary[IO],
    )

    val compressor = Compressor[IO]()
    val compressed = compressor.compress(ContentCoding.gzip)(entity)

    compressed.headers.get[`Content-Encoding`] shouldBe Some(`Content-Encoding`(ContentCoding.gzip))
    compressed.headers.get[`Content-Length`] shouldBe None
    compressed.headers.get[`Content-Type`] shouldBe entity.headers.get[`Content-Type`]
    compressed.body.through(
      compressor.decompress(ContentCoding.gzip)
    ).compile.toList.unsafeRunSync() shouldBe bytes
  }

  test("gzip unary encoders omit the length of the uncompressed body") {
    val message = Error(code = Code.Internal, message = Some("compressed"))
    val options = EncodeOptions(StandardCharsets.UTF_8, ContentCoding.gzip)
    val codecs  = List[MessageCodec[IO]](JsonSerdesBuilder[IO]().build.codec, new ProtoMessageCodec[IO])

    codecs.foreach { codec =>
      val roundTrip = for {
        encoded <- codec.encode(message, options)
        decoded <- EntityToDecode(encoded.body, encoded.headers).decodeOne(Error, codec)
      } yield {
        encoded.headers.get[`Content-Length`] shouldBe None
        encoded.headers.get[`Content-Encoding`] shouldBe Some(`Content-Encoding`(ContentCoding.gzip))
        decoded shouldBe message
      }
      roundTrip.unsafeRunSync()
    }
  }
}

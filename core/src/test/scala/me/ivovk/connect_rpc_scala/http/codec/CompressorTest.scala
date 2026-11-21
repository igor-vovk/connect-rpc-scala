package me.ivovk.connect_rpc_scala.http.codec

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import connectrpc.{Code, Error}
import fs2.Stream
import me.ivovk.connect_rpc_scala.ConnectException
import org.http4s.headers.{`Content-Encoding`, `Content-Length`, `Content-Type`}
import org.http4s.{ContentCoding, Header, Headers, MediaType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.*

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.zip.InflaterInputStream

class CompressorTest extends AnyFunSuite with Matchers {

  for (encoding <- List(ContentCoding.gzip, ContentCoding.deflate)) {
    test(s"${encoding.coding} updates encoding and removes the original content length") {
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
      val compressed = compressor.compress(encoding)(entity).unsafeRunSync()

      compressed.headers.get[`Content-Encoding`] shouldBe Some(`Content-Encoding`(encoding))
      compressed.headers.get[`Content-Length`] shouldBe None
      compressed.headers.get[`Content-Type`] shouldBe entity.headers.get[`Content-Type`]
      compressed.body.through(
        compressor.decompress(encoding)
      ).compile.toList.unsafeRunSync() shouldBe bytes
    }

    test(s"${encoding.coding} unary encoders omit the length of the uncompressed body") {
      val message = Error(code = Code.Internal, message = Some("compressed"))
      val options = EncodeOptions(StandardCharsets.UTF_8, encoding)
      val codecs  = List[MessageCodec[IO]](JsonSerdesBuilder[IO]().build.codec, new ProtoMessageCodec[IO])

      codecs.foreach { codec =>
        val roundTrip = for {
          encoded <- codec.encode(message, options)
          decoded <- EntityToDecode(encoded.body, encoded.headers).decodeOne(Error, codec)
        } yield {
          encoded.headers.get[`Content-Length`] shouldBe None
          encoded.headers.get[`Content-Encoding`] shouldBe Some(`Content-Encoding`(encoding))
          decoded shouldBe message
        }
        roundTrip.unsafeRunSync()
      }
    }
  }

  test("deflate uses the zlib wrapper") {
    val bytes  = List[Byte](1, 2, 3)
    val entity = EncodedEntity[IO](Headers.empty, Stream.emits(bytes))
    val result = Compressor[IO]().compress(ContentCoding.deflate)(entity)
      .flatMap(_.body.compile.to(Array)).unsafeRunSync()

    val inflater = new InflaterInputStream(new ByteArrayInputStream(result))
    try inflater.readAllBytes().toList shouldBe bytes
    finally inflater.close()
  }

  test("unsupported outbound compression fails with an unimplemented error") {
    val compressor = Compressor[IO]()
    val entity     = EncodedEntity[IO](Headers.empty, Stream.empty)
    val encoded    = compressor.compress(ContentCoding.br)(entity)

    val error = intercept[ConnectException](encoded.unsafeRunSync())

    error.code shouldBe Code.Unimplemented
    error.getMessage shouldBe "Unsupported compression br. Supported encodings are identity, gzip, and deflate."
  }
}

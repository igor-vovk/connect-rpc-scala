package org.ivovk.connect_rpc_scala.syntax

import io.grpc.Metadata.*

object metadata extends MetadataSyntax

trait MetadataSyntax {

  given BinaryMarshaller[Array[Byte]] = BINARY_BYTE_MARSHALLER

  given AsciiMarshaller[String] = ASCII_STRING_MARSHALLER

  def binaryMarshaller[A](decode: Array[Byte] => A)(encode: A => Array[Byte]): BinaryMarshaller[A] =
    new BinaryMarshaller[A] {
      override def toBytes(value: A): Array[Byte] = encode(value)

      override def parseBytes(serialized: Array[Byte]): A = decode(serialized)
    }

  def asciiMarshaller[A](decode: String => A)(encode: A => String): AsciiMarshaller[A] =
    new AsciiMarshaller[A] {
      override def toAsciiString(value: A): String = encode(value)

      override def parseAsciiString(serialized: String): A = decode(serialized)
    }

  extension [A](marshaller: BinaryMarshaller[A]) {
    def imap[B](f: A => B)(g: B => A): BinaryMarshaller[B] = new BinaryMarshaller[B] {
      override def toBytes(value: B): Array[Byte] = marshaller.toBytes(g(value))

      override def parseBytes(serialized: Array[Byte]): B = f(marshaller.parseBytes(serialized))
    }
  }

  extension [A](marshaller: AsciiMarshaller[A]) {
    def imap[B](f: A => B)(g: B => A): AsciiMarshaller[B] = new AsciiMarshaller[B] {
      override def toAsciiString(value: B): String = marshaller.toAsciiString(g(value))

      override def parseAsciiString(serialized: String): B = f(marshaller.parseAsciiString(serialized))
    }
  }

  def asciiKey[A](name: String)(using marshaller: AsciiMarshaller[A]): Key[A] =
    Key.of[A](name, marshaller)

  def binaryKey[A](name: String)(using marshaller: BinaryMarshaller[A]): Key[A] =
    Key.of[A](name, marshaller)

}

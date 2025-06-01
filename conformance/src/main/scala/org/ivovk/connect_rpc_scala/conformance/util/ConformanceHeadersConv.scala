package org.ivovk.connect_rpc_scala.conformance.util

import connectrpc.conformance.v1.Header
import io.grpc.Metadata
import org.ivovk.connect_rpc_scala.syntax.all.{asciiKey, given}

import scala.jdk.CollectionConverters.given

object ConformanceHeadersConv {
  private val TrailingHeaderPrefix = "trailer-"

  def toHeaderSeq(metadata: Metadata): Seq[Header] =
    metadata.keys().asScala
      .flatMap { key =>
        if key.startsWith(TrailingHeaderPrefix) then None // Skip trailing headers
        else Some(Header(key, metadata.getAll(asciiKey[String](key)).asScala.toSeq))
      }
      .toSeq

  def toTrailingHeaderSeq(metadata: Metadata): Seq[Header] =
    metadata.keys().asScala
      .flatMap { key =>
        if key.startsWith(TrailingHeaderPrefix) then
          val trailerKey = key.stripPrefix(TrailingHeaderPrefix)
          Some(Header(trailerKey, metadata.getAll(asciiKey[String](key)).asScala.toSeq))
        else None
      }
      .toSeq

  def toMetadata(headers: Seq[Header]): Metadata = {
    val metadata = new Metadata()
    headers.foreach { h =>
      val key = asciiKey[String](h.name)

      h.value.foreach { v =>
        metadata.put(key, v)
      }
    }
    metadata
  }
}

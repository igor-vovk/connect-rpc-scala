package org.ivovk.connect_rpc_scala.conformance.util

import connectrpc.conformance.v1.Header
import io.grpc.Metadata
import org.ivovk.connect_rpc_scala.syntax.all.{asciiKey, given}

import scala.jdk.CollectionConverters.given

object ConformanceHeadersConv {

  def toHeaderSeq(metadata: Metadata): Seq[Header] =
    metadata.keys().asScala
      .map { key =>
        Header(key, metadata.getAll(asciiKey[String](key)).asScala.toSeq)
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

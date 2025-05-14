package org.ivovk.connect_rpc_scala.netty.headers

import io.grpc.Metadata
import io.netty.handler.codec.http.{DefaultHttpHeaders, EmptyHttpHeaders, HttpHeaders}
import org.ivovk.connect_rpc_scala.http.{HeaderMapping, HeadersFilter}

class NettyHeaderMapping(
  headersFilter: HeadersFilter,
  metadataFilter: HeadersFilter,
  treatTrailersAsHeaders: Boolean,
) extends HeaderMapping[HttpHeaders] {
  import HeaderMapping.*

  override def toMetadata(headers: HttpHeaders): Metadata = {
    val metadata = new Metadata()
    headers.forEach { entry =>
      val name  = entry.getKey
      val value = entry.getValue

      if (headersFilter(name)) {
        metadata.put(metadataKeyByHeaderName(name), value)
      }
    }

    metadata
  }

  private def headers(
    metadata: Metadata,
    trailing: Boolean = false,
  ): HttpHeaders = {
    val keys = metadata.keys()
    if (keys.isEmpty) return EmptyHttpHeaders.INSTANCE

    val headers = new DefaultHttpHeaders()

    keys.forEach { key =>
      if (metadataFilter(key)) {
        val name = if (trailing) s"trailer-$key" else key

        headers.add(name, metadata.getAll(cachedAsciiKey(key)))
      }
    }

    headers
  }

  override def toHeaders(metadata: Metadata): HttpHeaders = headers(metadata)

  override def trailersToHeaders(metadata: Metadata): HttpHeaders =
    headers(metadata, trailing = !treatTrailersAsHeaders)
}

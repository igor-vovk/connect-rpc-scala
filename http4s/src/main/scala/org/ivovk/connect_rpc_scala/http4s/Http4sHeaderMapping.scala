package org.ivovk.connect_rpc_scala.http4s

import io.grpc.Metadata
import org.http4s.{Header, Headers}
import org.ivovk.connect_rpc_scala.{HeaderMapping, HeadersFilter}
import org.typelevel.ci.CIString

class Http4sHeaderMapping(
  headersFilter: HeadersFilter,
  metadataFilter: HeadersFilter,
  treatTrailersAsHeaders: Boolean,
) extends HeaderMapping[Headers] {
  import HeaderMapping.*

  override def toMetadata(headers: Headers): Metadata = {
    val metadata = new Metadata()
    headers.headers.foreach { header =>
      val headerName = header.name.toString
      if (headersFilter(headerName)) {
        metadata.put(metadataKeyByHeaderName(headerName), header.value)
      }
    }
    metadata
  }

  private def headers(
    metadata: Metadata,
    trailing: Boolean = false,
  ): Headers = {
    val keys = metadata.keys()
    if (keys.isEmpty) return Headers.empty

    val b = List.newBuilder[Header.Raw]

    keys.forEach { key =>
      if (metadataFilter(key)) {
        val name = if (trailing) CIString(s"trailer-$key") else CIString(key)

        metadata.getAll(cachedAsciiKey(key)).forEach { value =>
          b += Header.Raw(name, value)
        }
      }
    }

    new Headers(b.result())
  }

  override def toHeaders(metadata: Metadata): Headers =
    headers(metadata)

  override def trailersToHeaders(metadata: Metadata): Headers =
    headers(metadata, trailing = !treatTrailersAsHeaders)

}

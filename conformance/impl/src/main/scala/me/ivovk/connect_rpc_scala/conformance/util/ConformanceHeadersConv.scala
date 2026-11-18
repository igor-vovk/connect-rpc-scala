package me.ivovk.connect_rpc_scala.conformance.util

import connectrpc.conformance.v1.Header as ConformanceHeader
import org.http4s.{Header, Headers}
import org.typelevel.ci.CIString

import java.util.Locale

object ConformanceHeadersConv {

  def toHeaderSeq(headers: Headers): Seq[ConformanceHeader] =
    headers.headers.groupBy(_.name).iterator.map { (name, values) =>
      ConformanceHeader(name.toString.toLowerCase(Locale.ROOT), values.map(_.value))
    }.toList

  def toHttp4sHeaders(headers: Seq[ConformanceHeader]): Headers =
    Headers(headers.toList.flatMap { header =>
      header.value.map(value => Header.Raw(CIString(header.name), value))
    })
}

package org.ivovk.connect_rpc_scala.http

type HeadersFilter = String => Boolean

object HeaderMapping {
  val DefaultIncomingHeadersFilter: HeadersFilter = name =>
    !name.toLowerCase(java.util.Locale.ROOT).startsWith("connection")

  val DefaultOutgoingHeadersFilter: HeadersFilter = _ => true
}

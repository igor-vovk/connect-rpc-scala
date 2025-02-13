package org.ivovk.connect_rpc_scala

import io.grpc.Metadata
import org.ivovk.connect_rpc_scala.grpc.GrpcHeaders
import org.ivovk.connect_rpc_scala.syntax.all.{*, given}

import scala.collection.mutable

type HeadersFilter = String => Boolean

trait HeadersToMetadata[H] {
  def toMetadata(headers: H): Metadata
}

trait MetadataToHeaders[H] {
  def toHeaders(metadata: Metadata): H
  def trailersToHeaders(metadata: Metadata): H
}

object HeaderMapping {
  val DefaultIncomingHeadersFilter: HeadersFilter = name =>
    !(name.startsWith("Connection") || name.startsWith("connection"))

  val DefaultOutgoingHeadersFilter: HeadersFilter = name => !name.startsWith("grpc-")

  private val keyCache: mutable.Map[String, Metadata.Key[String]] =
    new mutable.WeakHashMap[String, Metadata.Key[String]]()

  def cachedAsciiKey(name: String): Metadata.Key[String] =
    keyCache.getOrElseUpdate(name, asciiKey(name))

  def metadataKeyByHeaderName(name: String): Metadata.Key[String] =
    name match {
      // Rename `User-Agent` to `x-user-agent` because `user-agent` is overridden by gRPC
      case "User-Agent" | "user-agent" =>
        GrpcHeaders.XUserAgentKey
      case _ => cachedAsciiKey(name)
    }

}

trait HeaderMapping[H] extends HeadersToMetadata[H], MetadataToHeaders[H]

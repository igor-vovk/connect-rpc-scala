package me.ivovk.connect_rpc_scala

import org.http4s.Headers
import org.typelevel.vault.{InsertKey, Vault}

import scala.concurrent.duration.FiniteDuration

final case class ConnectRequestContext(
  headers: Headers = Headers.empty,
  timeout: Option[FiniteDuration] = None,
  attributes: Vault = ConnectRequestContext.emptyAttributes,
) {
  require(headers.removePayloadHeaders == headers, "Payload headers can not be passed")

  /** Looks up a request-local value using the reader for its type. */
  def attribute[A](using reader: AttributeReader[A]): Option[A] =
    reader.read(attributes)

  /** Returns a context with the value stored under the given key, replacing any previous value. */
  def withAttribute[A](key: InsertKey[A], value: A): ConnectRequestContext =
    copy(attributes = attributes.insert(key, value))
}

object ConnectRequestContext {
  private val emptyAttributes = Vault.empty
}

package me.ivovk.connect_rpc_scala

import org.typelevel.vault.{LookupKey, Vault}

/** Extracts a typed, request-local value from a Connect call's attributes. */
trait AttributeReader[A] {
  def read(attributes: Vault): Option[A]
}

object AttributeReader {
  def fromKey[A](key: LookupKey[A]): AttributeReader[A] =
    _.lookup(key)
}

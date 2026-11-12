package me.ivovk.connect_rpc_scala.service

import java.util.Locale

sealed trait ConnectHeaders {

  def entries: Iterable[(String, Seq[String])]

  def get(name: String): Option[Seq[String]]

  def add(name: String, value: String): ConnectHeaders

  def ++(other: ConnectHeaders): ConnectHeaders
}

object ConnectHeaders {

  private final case class Impl(values: Map[String, List[String]]) extends ConnectHeaders {

    override def entries: Iterable[(String, Seq[String])] = values

    override def get(name: String): Option[Seq[String]] =
      values.get(normalize(name))

    override def add(name: String, value: String): ConnectHeaders = {
      val normalized = normalize(name)
      Impl(values.updatedWith(normalized)(existing => Some(existing.getOrElse(List.empty) :+ value)))
    }

    override def ++(other: ConnectHeaders): ConnectHeaders =
      other match {
        case Impl(otherValues) =>
          Impl(
            otherValues.foldLeft(values) { case (result, (name, newValues)) =>
              result.updatedWith(name)(existing => Some(existing.getOrElse(List.empty) ++ newValues))
            }
          )
      }
  }

  val empty: ConnectHeaders = Impl(Map.empty)

  def create(headers: Iterable[(String, String)]): ConnectHeaders =
    headers.foldLeft(empty) { case (result, (name, value)) => result.add(name, value) }

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)
}

package me.ivovk.connect_rpc_scala.http4s.headers

import org.http4s.{Header, ParseResult}
import org.typelevel.ci.*

import scala.concurrent.duration.*

private[http4s] final case class `Connect-Timeout-Ms`(timeout: FiniteDuration)

private[http4s] object `Connect-Timeout-Ms` {

  def parse(value: String): ParseResult[`Connect-Timeout-Ms`] =
    value.toLongOption match {
      case Some(milliseconds) => ParseResult.success(`Connect-Timeout-Ms`(milliseconds.millis))
      case None               => ParseResult.fail("Invalid Connect-Timeout-Ms header", value)
    }

  implicit val headerInstance: Header[`Connect-Timeout-Ms`, Header.Single] =
    Header.createRendered(
      ci"Connect-Timeout-Ms",
      _.timeout.toMillis,
      parse,
    )
}

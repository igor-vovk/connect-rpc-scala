package org.ivovk.connect_rpc_scala

import cats.Endo
import io.grpc.{ManagedChannelBuilder, ServerBuilder}
import scalapb.json4s.Printer

import scala.concurrent.duration.*

object Configuration {
  val default: Configuration = Configuration()
}

case class Configuration private(
  jsonPrinterConfigurator: Endo[Printer] = identity,
  serverBuilderConfigurator: Endo[ServerBuilder[_]] = identity,
  channelBuilderConfigurator: Endo[ManagedChannelBuilder[_]] = identity,
  waitForShutdown: Duration = 5.seconds,
) {

  def withJsonPrinterConfigurator(jsonPrinterConfigurer: Endo[Printer]): Configuration =
    copy(jsonPrinterConfigurator = jsonPrinterConfigurer)

  def withServerBuilderConfigurator(serverBuilderConfigurer: Endo[ServerBuilder[_]]): Configuration =
    copy(serverBuilderConfigurator = serverBuilderConfigurer)

  def withChannelBuilderConfigurator(channelBuilderConfigurer: Endo[ManagedChannelBuilder[_]]): Configuration =
    copy(channelBuilderConfigurator = channelBuilderConfigurer)

  def withWaitForShutdown(waitForShutdown: Duration): Configuration =
    copy(waitForShutdown = waitForShutdown)

}


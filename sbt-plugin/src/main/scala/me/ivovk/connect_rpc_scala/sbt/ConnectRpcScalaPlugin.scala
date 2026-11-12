package me.ivovk.connect_rpc_scala.sbt

import sbt._
import sbt.Keys._
import sbtprotoc.ProtocPlugin
import sbtprotoc.ProtocPlugin.ProtobufConfig
import sbtprotoc.ProtocPlugin.autoImport.PB

/** Generates ScalaPB messages and Connect service definitions for protobuf sources. */
object ConnectRpcScalaPlugin extends AutoPlugin {

  object autoImport {
    val connectRpcScalaVersion = settingKey[String](
      "The connect-rpc-scala generator and runtime version."
    )
    val connectRpcScalaGeneratorOptions = settingKey[Seq[String]](
      "Options passed to the connect-rpc-scala protoc generator."
    )
  }

  import autoImport._

  override def requires: Plugins = ProtocPlugin

  override def trigger: PluginTrigger = noTrigger

  override lazy val projectSettings: Seq[Def.Setting[_]] = Seq(
    connectRpcScalaVersion          := BuildInfo.version,
    connectRpcScalaGeneratorOptions := Seq("scala3_sources"),
    libraryDependencies ++= Seq(
      generatorDependency(connectRpcScalaVersion.value),
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "me.ivovk" %% "connect-rpc-scala-core" % connectRpcScalaVersion.value,
    ),
    ProtobufConfig / managedClasspath ~= (_.map { entry =>
      if (entry.data.getName.startsWith(s"$GeneratorName-"))
        entry.get(artifact.key).fold(entry)(resolved =>
          entry.put(artifact.key, resolved.withType(PB.ProtocPlugin))
        )
      else entry
    }),
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (PB.gens.plugin("connect-rpc-scala"), connectRpcScalaGeneratorOptions.value) ->
        (Compile / sourceManaged).value / "connect",
    ),
  )

  private val GeneratorName = "protoc-gen-connect-rpc-scala"

  private def generatorDependency(version: String): ModuleID = {
    val (classifier, extension) =
      if (scala.util.Properties.isWin) "windows" -> "bat"
      else "unix"                                -> "sh"

    ("me.ivovk" % GeneratorName % version % "protobuf").artifacts(
      Artifact(GeneratorName, PB.ProtocPlugin, extension, classifier)
    )
  }
}

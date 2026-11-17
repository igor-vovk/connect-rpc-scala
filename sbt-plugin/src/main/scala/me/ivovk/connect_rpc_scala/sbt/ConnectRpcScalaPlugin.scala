package me.ivovk.connect_rpc_scala.sbt

import sbt._
import sbt.Keys._
import sbtprotoc.ProtocPlugin
import sbtprotoc.ProtocPlugin.ProtobufConfig
import sbtprotoc.ProtocPlugin.autoImport.PB
import sbtcompat.{PluginCompat => SbtCompat}
import SbtCompat._

import java.io.File
import protocbridge.ProtocRunner

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

  override lazy val projectSettings = Seq(
    connectRpcScalaVersion          := BuildInfo.version,
    connectRpcScalaGeneratorOptions := Seq("scala3_sources"),
    libraryDependencies ++= Seq(
      generatorDependency(connectRpcScalaVersion.value),
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "me.ivovk" %% "connect-rpc-scala-core" % connectRpcScalaVersion.value,
    ),
    ProtobufConfig / managedClasspath := Def.uncached {
      implicit val converter: xsbti.FileConverter = fileConverter.value
      (ProtobufConfig / managedClasspath).value.map { entry =>
        if (SbtCompat.toFile(entry).getName.startsWith(s"$GeneratorName-"))
          entry.get(SbtCompat.artifactStr).fold(entry)(encodedArtifact =>
            entry.put(
              SbtCompat.artifactStr,
              SbtCompat.artifactToStr(
                SbtCompat.parseArtifactStrAttribute(encodedArtifact).withType(PB.ProtocPlugin)
              ),
            )
          )
        else entry
      }
    },
    Compile / PB.runProtoc ~= (withJavaOnPath(_)),
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = false, scala3Sources = true) ->
        (Compile / sourceManaged).value / "scalapb",
      (PB.gens.plugin("connect-rpc-scala"), connectRpcScalaGeneratorOptions.value) ->
        (Compile / sourceManaged).value / "connect",
    ),
  )

  private val GeneratorName = "protoc-gen-connect-rpc-scala"

  private[sbt] def withJavaOnPath(
    runner: ProtocRunner[Int],
    javaHome: File = file(sys.props("java.home")),
    inheritedPath: Option[String] = sys.env.get("PATH"),
  ): ProtocRunner[Int] =
    ProtocRunner.fromFunction { (arguments, environment) =>
      val configuredPath = environment.collectFirst {
        case (key, value) if key.equalsIgnoreCase("PATH") => value
      }.orElse(inheritedPath)
      val javaBin = (javaHome / "bin").getAbsolutePath
      val path    = configuredPath.filter(_.nonEmpty).fold(javaBin) { existingPath =>
        s"$javaBin${File.pathSeparator}$existingPath"
      }
      val updatedEnvironment = environment.filterNot { case (key, _) =>
        key.equalsIgnoreCase("PATH")
      } :+ ("PATH" -> path)

      runner.run(arguments, updatedEnvironment)
    }

  private def generatorDependency(version: String): ModuleID = {
    val (classifier, extension) =
      if (scala.util.Properties.isWin) "windows" -> "bat"
      else "unix"                                -> "sh"

    ("me.ivovk" % GeneratorName % version % "protobuf").artifacts(
      Artifact(GeneratorName, PB.ProtocPlugin, extension, classifier)
    )
  }
}

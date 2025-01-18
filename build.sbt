import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / scalaVersion := "3.3.4"

ThisBuild / organization := "me.ivovk"

ThisBuild / homepage := Some(url("https://github.com/igor-vovk/connect-rpc-scala"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "igor-vovk",
    "Ihor Vovk",
    "ideals-03.gushing@icloud.com",
    url("https://ivovk.me"),
  )
)
ThisBuild / sonatypeCredentialHost := xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / tpolecatExcludeOptions ++= Set(
  ScalacOptions.warnNonUnitStatement,
  ScalacOptions.warnValueDiscard,
)

lazy val noPublish = List(
  publish         := {},
  publishLocal    := {},
  publishArtifact := false,
  publish / skip  := true,
)

lazy val Versions = new {
  val grpc    = "1.69.0"
  val http4s  = "0.23.30"
  val logback = "1.5.16"
  val scalapb = _root_.scalapb.compiler.Version.scalapbVersion
}

lazy val core = project
  .settings(
    name := "connect-rpc-scala-core",
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value
    ),
    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0" % "protobuf",
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0",
      "com.thesamet.scalapb" %% "scalapb-runtime"      % Versions.scalapb % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % Versions.scalapb,
      "com.thesamet.scalapb" %% "scalapb-json4s"       % "0.12.1",
      "io.grpc"               % "grpc-core"            % Versions.grpc,
      "io.grpc"               % "grpc-protobuf"        % Versions.grpc,
      "io.grpc"               % "grpc-inprocess"       % Versions.grpc,
      "org.http4s"           %% "http4s-dsl"           % Versions.http4s,
      "org.http4s"           %% "http4s-client"        % Versions.http4s  % Test,
      "org.scalatest"        %% "scalatest"            % "3.2.19"         % Test,
      "ch.qos.logback"        % "logback-classic"      % Versions.logback % Test,
    ),
  )

lazy val conformance = project
  .dependsOn(core)
  .enablePlugins(Fs2Grpc, JavaAppPackaging)
  .settings(
    noPublish,
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val root = (project in file("."))
  .aggregate(
    core,
    conformance,
  )
  .settings(
    name := "connect-rpc-scala",
    noPublish,
  )

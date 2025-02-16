import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / scalaVersion := "3.3.5"

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
  val grpc      = "1.70.0"
  val http4s    = "0.23.30"
  val logback   = "1.5.16"
  val netty     = "4.1.118.Final"
  val scalapb   = _root_.scalapb.compiler.Version.scalapbVersion
  val scalatest = "3.2.19"
}

lazy val CommonDependencies = Seq(
  libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % Versions.logback   % Runtime,
    "org.scalatest" %% "scalatest"       % Versions.scalatest % Test,
  )
)

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
      // TODO: stop using http4s-core and remove the dependency from the module
      "org.http4s" %% "http4s-core" % Versions.http4s,
    ),
  )
  .settings(CommonDependencies)

lazy val http4s = project
  .dependsOn(core)
  .settings(
    name := "connect-rpc-scala-http4s",
    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value
    ),
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-dsl"    % Versions.http4s % Test,
      "org.http4s" %% "http4s-client" % Versions.http4s % Test,
    ),
  )
  .settings(CommonDependencies)

lazy val netty = project
  .dependsOn(core)
  .settings(
    name := "connect-rpc-scala-netty",
    libraryDependencies ++= Seq(
      // TODO: not needed in this form. It is here to switch to grpc-netty-shaded at some point
      "io.grpc"  % "grpc-netty" % Versions.grpc,
      "io.netty" % "netty-all"  % Versions.netty,
    ),
  )
  .settings(CommonDependencies)

lazy val conformance = project
  .dependsOn(http4s, netty)
  .enablePlugins(Fs2Grpc, JavaAppPackaging)
  .settings(
    noPublish,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % Versions.http4s
    ),
  )
  .settings(CommonDependencies)

lazy val root = (project in file("."))
  .aggregate(
    core,
    http4s,
    netty,
    conformance,
  )
  .settings(
    name := "connect-rpc-scala",
    noPublish,
  )

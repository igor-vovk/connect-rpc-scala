import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / scalaVersion := "3.3.6"

ThisBuild / organization := "me.ivovk"

ThisBuild / homepage   := Some(url("https://github.com/igor-vovk/connect-rpc-scala"))
ThisBuild / licenses   := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    "igor-vovk",
    "Ihor Vovk",
    "ideals-03.gushing@icloud.com",
    url("https://ivovk.me"),
  )
)

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
  val cedi      = "0.2.1"
  val grpc      = "1.73.0"
  val http4s    = "0.23.30"
  val logback   = "1.5.18"
  val netty     = "4.2.3.Final"
  val scalapb   = _root_.scalapb.compiler.Version.scalapbVersion
  val slf4j     = "2.0.17"
  val scalatest = "3.2.19"
}

lazy val commonDeps = Seq(
  libraryDependencies ++= Seq(
    "org.slf4j"      % "slf4j-api" % Versions.slf4j,
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test,
  )
)

lazy val core = project
  .settings(
    name                 := "connect-rpc-scala-core",
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value
    ),
    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value
    ),
    commonDeps,
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0" % "protobuf",
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0",
      "com.thesamet.scalapb" %% "scalapb-runtime"      % Versions.scalapb % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % Versions.scalapb,
      "com.thesamet.scalapb" %% "scalapb-json4s"       % "0.12.2",
      "io.grpc"               % "grpc-inprocess"       % Versions.grpc,
      // TODO: stop using http4s-core and remove the dependency from the module
      "org.http4s" %% "http4s-core" % Versions.http4s,
    ),
  )

lazy val http4s = project
  .dependsOn(core)
  .settings(
    name              := "connect-rpc-scala-http4s",
    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value
    ),
    commonDeps,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-dsl"    % Versions.http4s % Test,
      "org.http4s" %% "http4s-client" % Versions.http4s,
    ),
  )

lazy val netty = project
  .dependsOn(core)
  .settings(
    name := "connect-rpc-scala-netty",
    commonDeps,
    libraryDependencies ++= Seq(
      "io.netty" % "netty-all" % Versions.netty
    ),
  )

lazy val conformance = project
  .dependsOn(http4s, netty)
  .enablePlugins(Fs2Grpc, JavaAppPackaging)
  .settings(
    noPublish,
    commonDeps,
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val examples = project.in(file("examples"))
  .aggregate(
    example_connectrpc_grpc_servers,
    example_client_server,
    example_zio_client_server,
  )
  .settings(noPublish)

lazy val example_connectrpc_grpc_servers = project.in(file("examples/connectrpc_grpc_servers"))
  .dependsOn(http4s)
  .enablePlugins(Fs2Grpc)
  .settings(
    noPublish,
    commonDeps,
    libraryDependencies ++= Seq(
      "me.ivovk"      %% "cedi"                % Versions.cedi,
      "io.grpc"        % "grpc-netty"          % Versions.grpc,
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val example_client_server = project.in(file("examples/client_server"))
  .dependsOn(http4s, netty)
  .enablePlugins(Fs2Grpc, JavaAppPackaging)
  .settings(
    noPublish,
    commonDeps,
    libraryDependencies ++= Seq(
      "me.ivovk"      %% "cedi"                % Versions.cedi,
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val example_zio_client_server = project.in(file("examples/zio_client_server"))
  .dependsOn(http4s)
  .settings(
    noPublish,
    commonDeps,
    Compile / PB.targets := Seq(
      scalapb.gen()                     -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb",
    ),
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-ember-server" % Versions.http4s,
      "org.http4s"    %% "http4s-ember-client" % Versions.http4s,
      "dev.zio"       %% "zio"                 % "2.1.19",
      "dev.zio"       %% "zio-interop-cats"    % "23.1.0.5",
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val root = (project in file("."))
  .aggregate(
    core,
    http4s,
    netty,
    conformance,
    examples,
  )
  .settings(
    name := "connect-rpc-scala",
    noPublish,
  )

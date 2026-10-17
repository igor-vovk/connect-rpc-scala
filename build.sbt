import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / scalaVersion := "3.3.8"

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
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.78.0-1" % "protobuf",
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.78.0-1",
      "com.thesamet.scalapb"   %% "scalapb-runtime"      % Versions.scalapb % "protobuf",
      "com.thesamet.scalapb"   %% "scalapb-runtime-grpc" % Versions.scalapb,
      "io.github.scalapb-json" %% "scalapb-circe"        % "0.17.0",
      "co.fs2"                 %% "fs2-core"             % Versions.fs2,
      "co.fs2"                 %% "fs2-scodec"           % Versions.fs2,
      "co.fs2"                 %% "fs2-io"               % Versions.fs2,
      "io.grpc"                 % "grpc-inprocess"       % Versions.grpc,
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

lazy val conformance = project
  .dependsOn(http4s)
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

lazy val benchmarks = project
  .dependsOn(core)
  .enablePlugins(JmhPlugin)
  .settings(
    name := "connect-rpc-scala-benchmarks",
    noPublish,
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
  .dependsOn(http4s)
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
      "dev.zio"       %% "zio"                 % "2.1.26",
      "dev.zio"       %% "zio-interop-cats"    % "23.1.0.14",
      "ch.qos.logback" % "logback-classic"     % Versions.logback % Runtime,
    ),
  )

lazy val root = (project in file("."))
  .aggregate(
    core,
    http4s,
    conformance,
    benchmarks,
    examples,
  )
  .settings(
    name := "connect-rpc-scala",
    noPublish,
  )

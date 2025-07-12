addSbtPlugin("org.typelevel"  % "sbt-tpolecat"        % "0.5.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"      % "1.11.1")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")
addSbtPlugin("com.thesamet"   % "sbt-protoc"          % "1.0.8")
addSbtPlugin("org.typelevel"  % "sbt-fs2-grpc"        % "2.8.2")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"        % "2.5.5")

libraryDependencies += "com.thesamet.scalapb"          %% "compilerplugin"   % "0.11.19"
libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"

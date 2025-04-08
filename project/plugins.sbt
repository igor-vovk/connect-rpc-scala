addSbtPlugin("org.typelevel"  % "sbt-tpolecat"        % "0.5.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release"      % "1.9.3")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")
addSbtPlugin("com.thesamet"   % "sbt-protoc"          % "1.0.7")
addSbtPlugin("org.typelevel"  % "sbt-fs2-grpc"        % "2.8.0")
addSbtPlugin("org.scalameta"  % "sbt-scalafmt"        % "2.5.4")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

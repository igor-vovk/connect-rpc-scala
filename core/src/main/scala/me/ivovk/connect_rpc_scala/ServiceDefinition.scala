package me.ivovk.connect_rpc_scala

final case class ServiceDefinition[F[_]](
  name: String,
  unaryMethods: Seq[UnaryMethod[F]],
  serverStreamingMethods: Seq[ServerStreamingMethod[F]] = Seq.empty[ServerStreamingMethod[F]],
) {
  require(unaryMethods.forall(_.descriptor.serviceName == name), "All methods must belong to the service")
  require(
    serverStreamingMethods.forall(_.descriptor.serviceName == name),
    "All server-streaming methods must belong to the service",
  )
}

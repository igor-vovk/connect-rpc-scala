package org.ivovk.connect_rpc_scala.grpc

import com.google.api.http.HttpRule
import io.grpc.{MethodDescriptor, ServerMethodDefinition, ServerServiceDefinition}
import org.ivovk.connect_rpc_scala.grpc.MethodDescriptorExtensions.*
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

import scala.jdk.CollectionConverters.*

object MethodRegistry {

  case class Entry(
    name: MethodName,
    requestMessageCompanion: Companion[Message],
    httpRule: Option[HttpRule],
    descriptor: MethodDescriptor[Message, Message],
  )

  def apply(services: Seq[ServerServiceDefinition]): MethodRegistry = {
    val entries = services
      .flatMap(_.getMethods.asScala)
      .map(_.asInstanceOf[ServerMethodDefinition[Message, Message]])
      .map { methodDefinition =>
        val methodDescriptor = methodDefinition.getMethodDescriptor
        val requestCompanion = methodDescriptor.extractRequestMessageCompanionObj()
        val httpRule         = methodDescriptor.extractHttpRule()

        Entry(
          name = MethodName.from(methodDescriptor),
          requestMessageCompanion = requestCompanion,
          httpRule = httpRule,
          descriptor = methodDescriptor,
        )
      }

    new MethodRegistry(entries)
  }

}

class MethodRegistry private (entries: Seq[MethodRegistry.Entry]) {

  private val serviceMethodEntries: Map[Service, Map[Method, MethodRegistry.Entry]] = entries
    .groupMapReduce(_.name.service)(e => Map(e.name.method -> e))(_ ++ _)

  def all: Seq[MethodRegistry.Entry] = entries

  def get(name: MethodName): Option[MethodRegistry.Entry] = get(name.service, name.method)

  def get(service: Service, method: Method): Option[MethodRegistry.Entry] =
    serviceMethodEntries.getOrElse(service, Map.empty).get(method)

}

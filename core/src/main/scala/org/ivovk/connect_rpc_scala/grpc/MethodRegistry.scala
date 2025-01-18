package org.ivovk.connect_rpc_scala.grpc

import com.google.api.http.HttpRule
import io.grpc.{MethodDescriptor, ServerMethodDefinition, ServerServiceDefinition}
import scalapb.grpc.ConcreteProtoMethodDescriptorSupplier
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.jdk.CollectionConverters.*

object MethodRegistry {

  case class Entry(
    name: MethodName,
    requestMessageCompanion: GeneratedMessageCompanion[GeneratedMessage],
    httpRule: Option[HttpRule],
    descriptor: MethodDescriptor[GeneratedMessage, GeneratedMessage],
  )

  def apply(services: Seq[ServerServiceDefinition]): MethodRegistry = {
    val entries = services
      .flatMap(_.getMethods.asScala)
      .map(_.asInstanceOf[ServerMethodDefinition[GeneratedMessage, GeneratedMessage]])
      .map { smd =>
        val methodDescriptor = smd.getMethodDescriptor

        val requestMarshaller = methodDescriptor.getRequestMarshaller match
          case m: scalapb.grpc.Marshaller[_]               => m
          case tm: scalapb.grpc.TypeMappedMarshaller[_, _] => tm
          case unsupported => throw new RuntimeException(s"Unsupported marshaller $unsupported")

        val companionField = requestMarshaller.getClass.getDeclaredField("companion")
        companionField.setAccessible(true)

        val requestCompanion = companionField.get(requestMarshaller)
          .asInstanceOf[GeneratedMessageCompanion[GeneratedMessage]]

        val httpRule = extractHttpRule(methodDescriptor)

        Entry(
          name = MethodName.from(methodDescriptor),
          requestMessageCompanion = requestCompanion,
          httpRule = httpRule,
          descriptor = methodDescriptor,
        )
      }

    new MethodRegistry(entries)
  }

  private val HttpFieldNumber = 72295728

  private def extractHttpRule(methodDescriptor: MethodDescriptor[_, _]): Option[HttpRule] =
    methodDescriptor.getSchemaDescriptor match {
      case sd: ConcreteProtoMethodDescriptorSupplier =>
        val fields = sd.getMethodDescriptor.getOptions.getUnknownFields

        if fields.hasField(HttpFieldNumber) then
          Some(HttpRule.parseFrom(fields.getField(HttpFieldNumber).getLengthDelimitedList.get(0).toByteArray))
        else None
      case _ =>
        None
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

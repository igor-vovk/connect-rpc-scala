package org.ivovk.connect_rpc_scala.grpc

import com.google.api.AnnotationsProto
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
          case m: scalapb.grpc.Marshaller[_] => m
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
      .groupMapReduce(_.name.service)(e => Map(e.name.method -> e))(_ ++ _)

    new MethodRegistry(entries)
  }

  private def extractHttpRule(methodDescriptor: MethodDescriptor[_, _]): Option[HttpRule] = {
    methodDescriptor.getSchemaDescriptor match
      case sd: ConcreteProtoMethodDescriptorSupplier =>
        val fields      = sd.getMethodDescriptor.getOptions.getUnknownFields
        val fieldNumber = AnnotationsProto.http.getNumber

        if fields.hasField(fieldNumber) then
          Some(HttpRule.parseFrom(fields.getField(fieldNumber).getLengthDelimitedList.get(0).toByteArray))
        else None
      case _ => None
  }

}

class MethodRegistry private(entries: Map[Service, Map[Method, MethodRegistry.Entry]]) {

  def get(service: Service, method: Method): Option[MethodRegistry.Entry] =
    entries.getOrElse(service, Map.empty).get(method)

}

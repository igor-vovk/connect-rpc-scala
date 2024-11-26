package org.ivovk.connect_rpc_scala

import io.grpc.{MethodDescriptor, ServerMethodDefinition, ServerServiceDefinition}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.jdk.CollectionConverters.*

case class RegistryEntry(
  requestMessageCompanion: GeneratedMessageCompanion[GeneratedMessage],
  methodDescriptor: MethodDescriptor[GeneratedMessage, GeneratedMessage],
)

object MethodRegistry {

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

        val entry = RegistryEntry(
          requestMessageCompanion = requestCompanion,
          methodDescriptor = methodDescriptor,
        )

        methodDescriptor.getFullMethodName -> entry
      }
      .toMap

    new MethodRegistry(entries)
  }

}

class MethodRegistry private(entries: Map[String, RegistryEntry]) {

  def get(fullMethodName: String): Option[RegistryEntry] =
    entries.get(fullMethodName)

}

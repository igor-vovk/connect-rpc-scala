package org.ivovk.connect_rpc_scala.grpc

import io.grpc.{MethodDescriptor, ServerMethodDefinition, ServerServiceDefinition}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.jdk.CollectionConverters.*

object MethodRegistry {

  case class Entry(
    requestMessageCompanion: GeneratedMessageCompanion[GeneratedMessage],
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

        val methodEntry = Entry(
          requestMessageCompanion = requestCompanion,
          descriptor = methodDescriptor,
        )

        MethodName(methodDescriptor) -> methodEntry
      }
      .groupMapReduce((mn, _) => mn.service)((mn, m) => Map(mn.method -> m))(_ ++ _)

    new MethodRegistry(entries)
  }

}

class MethodRegistry private(entries: Map[Service, Map[Method, MethodRegistry.Entry]]) {

  def get(methodName: MethodName): Option[MethodRegistry.Entry] =
    entries.getOrElse(methodName.service, Map.empty).get(methodName.method)

}

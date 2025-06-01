package org.ivovk.connect_rpc_scala.grpc

import com.google.api.http.HttpRule
import io.grpc.MethodDescriptor
import scalapb.grpc.ConcreteProtoMethodDescriptorSupplier
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object MethodDescriptorExtensions {
  // Field number for the 'http' extension in google.protobuf.MethodOptions (see google/api/annotations.proto)
  private val HttpFieldNumber = 72295728

  extension (md: MethodDescriptor[_, _]) {

    def extractRequestMessageCompanionObj(): Companion[Message] =
      md.getRequestMarshaller match
        case m: scalapb.grpc.Marshaller[_]               => extractCompanionObj(m)
        case tm: scalapb.grpc.TypeMappedMarshaller[_, _] => extractCompanionObj(tm)
        case unsupported => throw new RuntimeException(s"Unsupported marshaller $unsupported")

    def extractResponseMessageCompanionObj(): Companion[Message] =
      md.getResponseMarshaller match
        case m: scalapb.grpc.Marshaller[_]               => extractCompanionObj(m)
        case tm: scalapb.grpc.TypeMappedMarshaller[_, _] => extractCompanionObj(tm)
        case unsupported => throw new RuntimeException(s"Unsupported marshaller $unsupported")

    private def extractCompanionObj(m: MethodDescriptor.Marshaller[_]): Companion[Message] = {
      val companionField = m.getClass.getDeclaredField("companion")
      if (!companionField.canAccess(m)) {
        companionField.setAccessible(true)
      }

      companionField.get(m).asInstanceOf[Companion[Message]]
    }

    def extractHttpRule(): Option[HttpRule] =
      md.getSchemaDescriptor match
        case sd: ConcreteProtoMethodDescriptorSupplier =>
          val fields = sd.getMethodDescriptor.getOptions.getUnknownFields

          if fields.hasField(HttpFieldNumber) then
            Some(
              HttpRule.parseFrom(fields.getField(HttpFieldNumber).getLengthDelimitedList.get(0).toByteArray)
            )
          else None
        case _ =>
          None

  }

}

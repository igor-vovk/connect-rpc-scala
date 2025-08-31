package org.ivovk.connect_rpc_scala.grpc

import com.google.api.AnnotationsProto
import com.google.api.http.HttpRule
import io.grpc.MethodDescriptor
import scalapb.grpc.ConcreteProtoMethodDescriptorSupplier
import scalapb.{GeneratedMessage as Message, GeneratedMessageCompanion as Companion}

object MethodDescriptorExtensions {

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
          val options       = sd.getMethodDescriptor.getOptions
          val unknownFields = options.getUnknownFields

          if unknownFields.hasField(AnnotationsProto.HTTP_FIELD_NUMBER) then
            val is = unknownFields.getField(AnnotationsProto.HTTP_FIELD_NUMBER)
              .getLengthDelimitedList.get(0)
              .newCodedInput()

            Some(HttpRule.parseFrom(is))
          else None
        case _ => None

  }

}

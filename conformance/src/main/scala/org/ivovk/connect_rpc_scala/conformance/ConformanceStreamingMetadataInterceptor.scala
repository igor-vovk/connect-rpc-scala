package org.ivovk.connect_rpc_scala.conformance

import connectrpc.conformance.v1.{BidiStreamRequest, ServerStreamRequest}
import io.grpc.*
import org.ivovk.connect_rpc_scala.conformance.util.ConformanceHeadersConv.*

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class ConformanceStreamingMetadataInterceptor extends ServerInterceptor {

  override def interceptCall[ReqT, RespT](
    call: ServerCall[ReqT, RespT],
    headers: Metadata,
    next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val initialHeaders  = new Metadata()
    val trailingHeaders = new Metadata()
    val headersSent     = AtomicBoolean(false)
    val metadataLoaded  = AtomicBoolean(false)
    val pendingHeaders  = AtomicReference[Metadata | Null](null)

    def flushHeaders(): Unit = {
      val headers = pendingHeaders.getAndSet(null)

      if headers != null && headersSent.compareAndSet(false, true) then
        mergeInto(headers, initialHeaders)
        call.sendHeaders(headers)
    }

    val wrappedCall = new ServerCall[ReqT, RespT] {
      override def request(numMessages: Int): Unit =
        call.request(numMessages)

      override def sendHeaders(responseHeaders: Metadata): Unit = {
        try
          pendingHeaders.set(responseHeaders)
          if metadataLoaded.get() then flushHeaders()
        catch
          case t: Throwable =>
            System.err.println(s"ConformanceStreamingMetadataInterceptor.sendHeaders failed: ${t.getMessage}")
            t.printStackTrace()
            throw t
      }

      override def sendMessage(message: RespT): Unit =
        call.sendMessage(message)

      override def close(status: Status, trailers: Metadata): Unit = {
        try
          mergeInto(trailers, trailingHeaders)
          call.close(status, trailers)
        catch
          case t: Throwable =>
            System.err.println(s"ConformanceStreamingMetadataInterceptor.close failed: ${t.getMessage}")
            t.printStackTrace()
            throw t
      }

      override def isCancelled: Boolean =
        call.isCancelled

      override def isReady: Boolean =
        call.isReady

      override def setMessageCompression(enabled: Boolean): Unit =
        call.setMessageCompression(enabled)

      override def setCompression(compressor: String): Unit =
        call.setCompression(compressor)

      override def getMethodDescriptor: MethodDescriptor[ReqT, RespT] =
        call.getMethodDescriptor

      override def getAttributes: Attributes =
        call.getAttributes

      override def getAuthority: String =
        call.getAuthority
    }

    val delegate = next.startCall(wrappedCall, headers)

    new ServerCall.Listener[ReqT] {
      override def onMessage(message: ReqT): Unit = {
        try
          extractResponseDefinition(message).foreach { definition =>
            if metadataLoaded.compareAndSet(false, true) then
              mergeInto(initialHeaders, toMetadata(definition.responseHeaders))
              mergeInto(trailingHeaders, toMetadata(definition.responseTrailers))
              flushHeaders()
          }

          delegate.onMessage(message)
        catch
          case t: Throwable =>
            System.err.println(s"ConformanceStreamingMetadataInterceptor.onMessage failed: ${t.getMessage}")
            t.printStackTrace()
            throw t
      }

      override def onHalfClose(): Unit =
        delegate.onHalfClose()

      override def onCancel(): Unit =
        delegate.onCancel()

      override def onComplete(): Unit =
        delegate.onComplete()

      override def onReady(): Unit =
        delegate.onReady()
    }
  }

  private def extractResponseDefinition[ReqT](
    message: ReqT
  ): Option[connectrpc.conformance.v1.StreamResponseDefinition] =
    message match
      case req: ServerStreamRequest => Some(req.getResponseDefinition)
      case req: BidiStreamRequest   => Some(req.getResponseDefinition)
      case _                        => None

  private def mergeInto(target: Metadata, source: Metadata): Unit =
    source.keys.forEach { key =>
      val metadataKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)
      val values      = source.getAll(metadataKey)

      if values != null then values.forEach(value => target.put(metadataKey, value))
    }
}

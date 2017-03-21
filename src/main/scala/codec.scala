package com.franz.franz2
import spray.json._
import DefaultJsonProtocol._
import org.jboss.netty.buffer._
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel._
case class FranzRequest(command: String, details: Map[String, String])

object codecUtils {
  implicit val jsf = jsonFormat2(FranzRequest)
  val decodeRequest = (req: String )=> req.parseJson.convertTo[FranzRequest]
  val encodeRequest = (req: FranzRequest) => req.toJson.toString.getBytes()
  def MyEncoder = new OneToOneEncoder { override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = ChannelBuffers.copiedBuffer(encodeRequest(msg.asInstanceOf[FranzRequest]))}
  def MyDecoder = new OneToOneDecoder { override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: Any) = decodeRequest(msg.toString) }
  def serverPipeline =  {
      val sp = Channels.pipeline()
      sp.addLast("strings", new StringDecoder()); sp.addLast("protocolDecoder", MyDecoder); sp.addLast("Resp", new StringEncoder())
      sp
  }
  def clientPipeline = {
      val cp = Channels.pipeline()
      cp.addLast("encode", MyEncoder); cp.addLast("Response", new StringDecoder())
      cp
  }
}

class FranzProtocol extends CodecFactory[FranzRequest, String] {
  import codecUtils._
  def server = Function.const { new Codec[FranzRequest, String] { def pipelineFactory = new ChannelPipelineFactory { def getPipeline = { serverPipeline } } } }
  def client = Function.const { new Codec[FranzRequest, String] { def pipelineFactory = new ChannelPipelineFactory { def getPipeline = { clientPipeline } } } }
}

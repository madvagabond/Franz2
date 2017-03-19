import com.twitter.finagle._
import com.twitter.util._
import com.twitter.finagle.builder._
import spray.json._
import DefaultJsonProtocol._
object client {
  def sendMessage(addr: String)(msg: FranzRequest) = {
    //implicit val js = jsonFormat2(FranzRequest)
    val client = ClientBuilder().codec(new FranzProtocol()).hosts(addr).hostConnectionLimit(10).build()
    client(msg)
  }
  def publish(addr: String, topic: String, payload: String) = sendMessage(addr)(FranzRequest("Publish", Map("topic" -> topic, "payload" -> payload)))
  def subscribe[T](addr: String, topic: String)(f: String => T): Future[String] = sendMessage(addr)(FranzRequest("Subscribe", Map("topic" -> topic))) onSuccess {x => f(x); subscribe(addr, topic)(f)} 
  def createTopic(addr: String, topic: String) = sendMessage(addr)(FranzRequest("Create Topic", Map("topic" -> topic)))
}

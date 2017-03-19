import com.twitter.finagle._
import com.twitter.util._
import scala.collection.concurrent.TrieMap
import com.twitter.finagle.builder._
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue

class FQ extends Service[FranzRequest, String] {
  var table = new TrieMap[String, ConcurrentLinkedQueue[Promise[String]]]()
  var waiters = new TrieMap[String, ConcurrentLinkedQueue[String]]()
  def apply(req: FranzRequest): Future[String] = {
    lazy val topic = req.details.get("topic").get
    lazy val pl = req.details.get("payload").get
    req match {
      case FranzRequest("Publish", args) if (args.contains("topic") && args.contains("payload") && table.contains(topic) && !table.get(topic).get.isEmpty) =>
        val p = table.get(topic) match {case Some(x) => x.poll(); case None => new Promise[String]();}
        p.setValue(pl)
        p
      case FranzRequest("Publish", args) if (args.contains("topic") && args.contains("payload") && table.contains(topic) && table.get(topic).get.isEmpty) =>
        waiters.get(topic).get.offer(pl)
        Future("Published")
      case FranzRequest("Subscribe", args) if (args.contains("topic") && table.contains(topic) && !waiters.get(topic).get.isEmpty) =>
        Future(waiters.get(topic).get.poll)
      case FranzRequest("Subscribe", args) if (args.contains("topic") && table.contains(topic)) =>
        val p = new Promise[String]()
        table.get(topic).get.offer(p)
        p
      case FranzRequest("Create Topic", args) if (args.contains("topic") && !table.contains(topic)) =>
        table put(topic, new ConcurrentLinkedQueue[Promise[String]]())
        waiters put(topic, new ConcurrentLinkedQueue[String]())
        Future("Created")
      case FranzRequest("Publish", args) if (args.contains("topic") && args.contains("payload") && table.contains(topic)) =>
        waiters.get(topic).get.offer(pl)
        Future("published")
      case FranzRequest(a, b) =>
        Future("Unknown")
    }
  }
}

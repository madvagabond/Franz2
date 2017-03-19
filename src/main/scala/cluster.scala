import com.twitter.finagle.{Filter, Service, SimpleFilter}
import com.twitter.util._
import com.twitter.hashing._
import scala.collection.mutable.ListBuffer
import com.twitter.bijection._
import com.twitter.bijection.Conversion.asMethod
import java.net.InetSocketAddress
import spray.json._
import DefaultJsonProtocol._
case class membership(me: String, peerSet: ListBuffer[String])
class clusterMode[FranzRequest, String](peers: membership) extends SimpleFilter[FranzRequest, java.lang.String] {
  import utils._
  def apply(req: FranzRequest, service: Service[FranzRequest, java.lang.String]): Future[java.lang.String] = {
    implicit def tos(x: String) = x.as[String]
    val res = req match {
      case FranzRequest("Join Cluster", args) => Future{peers.peerSet.toList.toJson.toString}
      case FranzRequest("New Member Broadcast", args) if args.contains("address") =>
        synchronized {peers.peerSet :+ args.get("address").get}
        Future{"Added"}
      case FranzRequest(cmd, args) if args.contains("topic") =>
        val peer = getMember(peers.peerSet.toList, args.get("topic").get)
        if (peer != peers.me) client.sendMessage(peer)(FranzRequest(cmd, args)) else service(req)
    }
    res
  }
}
object utils {
  import com.twitter.finagle.builder._
  import scala.collection.concurrent.TrieMap
  import scala.util.Random
  def toKetama(addr: String) = KetamaNode(addr, 600, addr)
  def getMember(ps: List[String], key: String) = {
    val nodes = ps.map(x => toKetama(x))
    val dist = new KetamaDistributor(nodes, 160)
    dist.nodeForHash(key.##)
  }
  def clusteredFranz(me: String, peers: List[String]) = {
    val tbl = new TrieMap[String, TrieMap[String, Promise[String]]]()
    val myService = new clusterMode(membership(me, peers.to[ListBuffer])) andThen new FQ
    ServerBuilder().codec(new FranzProtocol()).bindTo(makeAddr(me)).name("franz2").build(myService)
  }
  def makeServer(me: String) = ServerBuilder().codec(new FranzProtocol()).bindTo(makeAddr(me)).name("franz2").build(new FQ())
  def makeAddr(addr: String) = new InetSocketAddress(addr.split(":")(1).toInt)
  def pickRandom(l: TrieMap[String, Promise[String]]) = {
    val r = new Random()
    val nl = l.keySet.toList
    nl(r.nextInt(nl.size))
  }
  def joinCluster(addr: String, bootstrapHost: String) = {
    import client._
    import com.twitter.util.FuturePool
    val j = sendMessage(bootstrapHost)(FranzRequest("Join Cluster", Map("address" -> addr)))
    val peers = j.get.parseJson.convertTo[List[String]]
    peers.foreach {x => sendMessage(x)(FranzRequest("New Member Broadcast", Map("address"-> addr)))}
    clusteredFranz(addr, peers)
  }
}

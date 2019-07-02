package hashing

import java.util
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger

import hashing.Hashing2.{Consistent, Hashing, Rendezvous}

//runMain hashing.CompareHashing2
object CompareHashing2 {

  val iterNum = 500000

  val nodes0 = "alpha" :: "beta" :: "gamma" :: "delta" :: Nil
  val nodes  = "alpha" :: "beta" :: "gamma" :: "delta" :: "epsilon" :: "zeta" :: "eta" :: "theta" :: "iota" :: "kappa" :: Nil

  val rnd = java.util.concurrent.ThreadLocalRandom.current

  def getNodes(distribution: util.Map[String, AtomicInteger]) = {
    val localNodes = new java.util.ArrayList[String]
    (0 until nodes.size).foreach { i ⇒
      val node = nodes(i)
      localNodes.add(node)
      distribution.put(node, new AtomicInteger)
    }
    localNodes
  }

  def line(limit: Int): String = {
    def go(sb: StringBuilder, i: Int, limit: Int): String =
      if (i < limit) go(sb.append(rnd.nextInt('a'.toInt, 'z'.toInt).toChar), i + 1, limit)
      else sb.toString

    go(new StringBuilder, 0, limit)
  }

  def iter0(router: akka.routing.ConsistentHash[String], distribution: Map[String, AtomicInteger]) = {
    (0 until iterNum).foreach { _ ⇒
      val key = line(10)
      distribution.get(router.nodeFor(key)).incrementAndGet
    }

    val iter = distribution.entrySet.iterator
    while (iter.hasNext) {
      val e = iter.next
      println(s"${e.getKey} - ${e.getValue.get}")
    }

    var updatedRouter = router

    println("====== remove ========")
    (0 until nodes.size / 2).foreach { i ⇒
      val node = nodes(i)
      updatedRouter = updatedRouter :- node
      distribution.remove(node)
      println("removed: " + node)
    }

    println("====== add after remove ========")
    (0 until iterNum).foreach { _ ⇒
      val key = line(10)
      distribution.get(updatedRouter.nodeFor(key)).incrementAndGet
    }

    println("====== stats ========")
    val iter1 = distribution.entrySet.iterator
    while (iter1.hasNext) {
      val e = iter1.next
      println(e.getKey + ", " + e.getValue.get)
    }
  }

  def iter(router: Hashing[String], distribution: Map[String, AtomicInteger]) = {
    (0 until iterNum).foreach { _ ⇒
      val key = line(10)
      distribution.get(router.nodeFor(key, 1).head).incrementAndGet
    }

    val iter = distribution.entrySet.iterator
    while (iter.hasNext) {
      val e = iter.next
      println(s"${e.getKey} - ${e.getValue.get}")
    }

    println("====== remove ========")
    (0 until nodes.size / 2).foreach { i ⇒
      val node = nodes(i)
      router.removeNode(node)
      distribution.remove(node)
    }

    println("====== add after remove ========")
    (0 until iterNum).foreach { _ ⇒
      val key = line(10)
      distribution.get(router.nodeFor(key, 1).head).incrementAndGet
    }

    println("====== stats ========")
    val iter1 = distribution.entrySet.iterator
    while (iter1.hasNext) {
      val e = iter1.next
      println(e.getKey + ", " + e.getValue.get)
    }
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis

    //HashingRouter[Rendezvous[String]]

    println("======: ConsistentHash :========")
    val consistentHist: Map[String, AtomicInteger] = new util.LinkedHashMap[String, AtomicInteger]()
    val ch                                         = Consistent[String].withNodes(getNodes(consistentHist))
    //println(ch.asInstanceOf[Consistent[String]].showDiff)
    iter(ch, consistentHist)

    println("======: RendezvousHashing :========")
    val rendezvousHist: Map[String, AtomicInteger] = new util.LinkedHashMap[String, AtomicInteger]()
    iter(Rendezvous[String].withNodes(getNodes(rendezvousHist)), rendezvousHist)

    println("======: Akka ConsistentHash :========")
    import scala.collection.JavaConverters._
    val consistentHistAkka: Map[String, AtomicInteger] = new util.LinkedHashMap[String, AtomicInteger]()
    iter0(akka.routing.ConsistentHash[String](getNodes(consistentHistAkka).asScala, 4), consistentHistAkka)

    //com.github.ssedano.hash.JumpConsistentHash.jumpConsistentHash(67l, 5)

    /*val ch0 = com.ctheu.ch.ConsistentHashing(replicaCount = 2).withNodes(nodes0.map(com.ctheu.ch.Node(_)) :_*)
    ch0.replicaCount == 3
    ch0.nodeCount == nodes.size
    ch0.lookup("a")*/


    val latency = System.currentTimeMillis - start
    println("==============")
    println("Latency: " + latency)
  }
}

package hashing

import java.util
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger

import hashing.Hashing2.{ Consistent, Hashing, Rendezvous }

//runMain hashing.CompareHashing2
object CompareHashing2 {

  val keysNum = 500000

  val nodes0 = "alpha" :: "beta" :: "gamma" :: "delta" :: Nil
  val nodes = "alpha" :: "beta" :: "gamma" :: "delta" :: "epsilon" :: "zeta" :: "eta" :: "theta" :: "iota" :: "kappa" :: Nil

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
    def go(sb: StringBuilder, i: Int, limit: Int): String = {
      if (i < limit) go(sb.append(rnd.nextInt('a'.toInt, 'z'.toInt).toChar), i + 1, limit)
      else sb.toString
    }
    go(new StringBuilder, 0, limit)
  }

  def iter(router: Hashing[String], distribution: Map[String, AtomicInteger]) = {
    (0 until keysNum).foreach { _ ⇒
      val key = line(10)
      distribution.get(router.nodeFor(key, 1).head).incrementAndGet
    }

    val iter = distribution.entrySet.iterator
    while (iter.hasNext) {
      val e = iter.next
      println(s"${e.getKey} - ${e.getValue.get}")
      //zeroing out
      //e.getValue.set(0)
    }

    println("====== remove ========")
    (0 until nodes.size / 2).foreach { i ⇒
      val node = nodes(i)
      router.removeNode(node)
      distribution.remove(node)
    }

    println("====== add after remove ========")
    (0 until keysNum).foreach { _ ⇒
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
    val consistentHist: Map[String, AtomicInteger] = new util.HashMap[String, AtomicInteger]()
    iter(Consistent[String].withNodes(getNodes(consistentHist)), consistentHist)

    println("======: RendezvousHashing :========")
    val rendezvousHist: Map[String, AtomicInteger] = new util.HashMap[String, AtomicInteger]()
    iter(Rendezvous[String].withNodes(getNodes(rendezvousHist)), rendezvousHist)

    /*
    println("======: MinHashing :========")
    val distribution2: Map[String, AtomicInteger] = new util.HashMap[String, AtomicInteger]()
    iter(MinHashing[String].withNodes(getNodes(distribution2)), distribution2)
    */

    val latency = System.currentTimeMillis - start
    println("==============")
    println("Latency: " + latency)
  }
}
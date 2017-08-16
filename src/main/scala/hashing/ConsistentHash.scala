package hashing

import java.util.{ SortedMap ⇒ JSortedMap, TreeMap ⇒ JTreeMap }

import scala.collection.immutable.LinearSeq

//https://gist.github.com/opyate/1927001

/**
 * Inspired by http://www.lexemetech.com/2007/11/consistent-hashing.html
 */
object ConsistentHash {

  def apply[K <% Ordered[K], V](hashFunction: HashFunction[K], numberOfReplicas: Int, nodes: LinearSeq[V]) = {
    val ch = new ConsistentHash(hashFunction, numberOfReplicas, nodes)
    nodes.foreach(n ⇒ ch.add(Some(n)))
    ch
  }
}

/**
 * HashFunction returns a hash of type K, which will be Ordered for the TreeMap
 * Nodes are of type V
 */
class ConsistentHash[K <% Ordered[K], V] private (hashFunction: HashFunction[K], numberOfReplicas: Int, nodes: LinearSeq[V]) {
  var numNodes = 0
  val SEPARATOR = ":"
  val ring: JSortedMap[K, V] = new JTreeMap[K, V]()

  /**
   * Add a node V to the pool.
   */
  def add(node: Option[V]) {
    node.map { n ⇒
      (0 to numberOfReplicas).foreach { i ⇒
        val hash = hashFunction.hash("%s%s%s".format(n, SEPARATOR, i))
        ring.put(hash, n)
      }
    }
    numNodes = numNodes + 1
  }

  /**
   * Remove a node V from the pool.
   */
  def remove(node: Option[V]) {
    node.map { n ⇒
      (0 to numberOfReplicas).foreach { i ⇒
        val hash = hashFunction.hash("%s%s%s".format(n, SEPARATOR, i))
        ring.remove(hash)
      }
    }
    numNodes = numNodes - 1
  }

  /**
   * @return the cache node that will contain our object, by key.
   */
  def get(key: Option[AnyRef]): Option[V] = ring.isEmpty match {
    case true ⇒ throw new RuntimeException("No nodes in ring")
    case false ⇒ {
      key.map { k ⇒
        hashFunction.hash(k) match {
          case hash if (!ring.containsKey(hash)) ⇒ {
            ring.tailMap(hash) match {
              case tailMap if (tailMap.isEmpty) ⇒ Some(ring.get(ring.firstKey))
              case tailMap                      ⇒ Some(ring.get(tailMap.firstKey))
            }
          }
          case hash ⇒ Some(ring.get(hash))
        }
      }.getOrElse(None)
    }
  }

  def ringSize(): Int = numNodes
}

trait HashFunction[H] {
  def hash(o: AnyRef): H
}

import java.io.{ ByteArrayOutputStream, ObjectOutputStream }
import java.math.BigInteger
import java.security.MessageDigest

class MD5HashFunction extends HashFunction[String] {

  val md: MessageDigest = MessageDigest.getInstance("MD5")

  override def hash(o: AnyRef): String = {
    byteArrayToString(md.digest(toBinary(o)))
  }

  private[this] def byteArrayToString(data: Array[Byte]): String = {
    val bigInteger = new BigInteger(1, data)
    var hash = bigInteger.toString(16)
    while (hash.length() < 32) {
      hash = "0" + hash
    }
    hash
  }

  private[this] def toHex(b: Byte): Char = {
    require(b >= 0 && b <= 15, "Byte " + b + " was not between 0 and 15")
    if (b < 10)
      ('0'.asInstanceOf[Int] + b).asInstanceOf[Char]
    else
      ('a'.asInstanceOf[Int] + (b - 10)).asInstanceOf[Char]
  }

  private[this] def toBinary(obj: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(obj)
    out.close
    bos.toByteArray
  }
}

import java.util.Random

/**
 * Drives our implementation, and generates some stats.
 */
class Runner {
  val quiet = true

  implicit def cacheAsString(cache: Option[Cache]): String = cache.map {
    _.toString
  }.getOrElse("-NOCACHE-")

  implicit def optionAnyToString(o: Option[Any]): String = o.map(_.toString).getOrElse("")

  def stat(ch: ConsistentHash[_, _], objects: IndexedSeq[CachableThingy]) = {
    (for { o ← objects } yield ch.get(Some(o))).groupBy(x ⇒ x).map { case (k, v) ⇒ v.size }
  }

  def sq(i: Double) = i * i

  def debug(s: String) {
    if (!quiet) println(s)
  }

  def write(data: List[Tuple2[Int, Double]]) {
    import java.io._

    def printToFile(f: java.io.File)(op: java.io.PrintWriter ⇒ Unit) {
      val p = new java.io.PrintWriter(f)
      try {
        op(p)
      } finally {
        p.close()
      }
    }

    printToFile(new File("dat"))(p ⇒ {
      data.foreach(tup ⇒ {
        p.println("%s\t%s".format(tup._1, tup._2))
      })
    })
  }

  def run(numobjects: Int, vnodes: Int) = {
    debug("\n\n%s objects, and %s vnodes".format(numobjects, vnodes))
    val ch = ConsistentHash(new MD5HashFunction, vnodes,
      List(
        Cache("alpha"),
        Cache("beta"),
        Cache("gamma"),
        Cache("delta"),
        Cache("epsilon"),
        Cache("zeta"),
        Cache("eta"),
        Cache("theta"),
        Cache("iota"),
        Cache("kappa")))
    val rand = new Random(System.currentTimeMillis())
    val objects: IndexedSeq[CachableThingy] = for { i ← 0 until numobjects } yield {
      CachableThingy(rand.nextInt(10000))
    }

    // mean: average
    // variance: average of the squared differences from the mean.
    // standard deviation: square root of the variance

    val stats = stat(ch, objects)
    // we already know the total number of objects, and can cheat to get the mean
    val mean = numobjects / ch.ringSize
    val sqDistFromMean = stats.map(opc ⇒ {
      debug("%s of %s / size(%s) = Mean(%s)".format(opc, numobjects, ch.ringSize, mean))
      sq(mean - opc)
    })
    debug("%s".format(sqDistFromMean))
    val variance: Double = sqDistFromMean.sum / ch.ringSize
    debug("Variance: %s".format(variance))
    val sd = scala.math.sqrt(variance)
    debug("Standard deviation: %s".format(sd))
    val sdm = (sd / mean) * 100
    debug("Standard deviation as a percentage of the mean: %s".format(sdm))
    debug("-")
    (vnodes, sdm)
  }
}

case class Cache(s: String)

case class CachableThingy(i: Int)

object CHApp extends App {
  val r = new Runner

  val stats = for { vnodes ← (1 to 500).toList } yield {
    print(" " + vnodes) // make-shift progress indicator
    r.run(10000, vnodes)
  }

  // writes to file called "dat" in '.'
  r.write(stats)
}
package hashing

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ConcurrentSkipListSet

import com.twitter.algebird.CassandraMurmurHash

import scala.collection.immutable.SortedSet
import scala.reflect.ClassTag
import java.nio.charset.StandardCharsets._

object Hashing2 {

  /*
    Data sharding where everything related to a given key to hit the same machine
   */
  @simulacrum.typeclass
  trait Hashing[Node] {
    def seed: Long
    def name: String

    def withNodes(nodes: util.Collection[Node]): Hashing[Node] = {
      val iter = nodes.iterator
      while (iter.hasNext) {
        addNode(iter.next)
      }
      this
    }

    def toBinary(node: Node): Array[Byte]

    def removeNode(node: Node): Boolean

    def addNode(node: Node): Boolean

    def nodeFor(key: String, rf: Int): Set[Node]

    def validated(node: Node): Boolean
  }

  /**
    * Highest Random Weight (HRW) hashing
    * https://github.com/clohfink/RendezvousHash
    * https://www.pvk.ca/Blog/2017/09/24/rendezvous-hashing-my-baseline-consistent-distribution-method/
    * A random uniform way to partition your keyspace up among the available nodes
    */
  @simulacrum.typeclass
  trait Rendezvous[Node] extends Hashing[Node] {
    override val seed  = 512L
    override val name  = "rendezvous-hashing"
    protected val ring = new ConcurrentSkipListSet[Node]()

    override def removeNode(node: Node): Boolean =
      ring.remove(node)

    override def addNode(node: Node): Boolean =
      if (validated(node)) ring.add(node) else false

    override def nodeFor(key: String, rf: Int): Set[Node] = {
      var candidates = SortedSet.empty[(Long, Node)]((x: (Long, Node), y: (Long, Node)) ⇒ -x._1.compare(y._1))
      val iter       = ring.iterator
      while (iter.hasNext) {
        val node           = iter.next
        val keyBytes       = key.getBytes(UTF_8)
        val nodeBytes      = toBinary(node)
        val byteBuffer     = ByteBuffer.allocate(keyBytes.length + nodeBytes.length).put(keyBytes).put(nodeBytes)
        val nodeHash128bit = CassandraMurmurHash.hash3_x64_128(byteBuffer, 0, byteBuffer.array.length, seed)(1)
        candidates = candidates + (nodeHash128bit → node)
      }
      candidates.take(rf).map(_._2)
    }
  }

  /*
    https://community.oracle.com/blogs/tomwhite/2007/11/27/consistent-hashing
    https://www.datastax.com/dev/blog/token-allocation-algorithm
    http://docs.basho.com/riak/kv/2.2.3/learn/concepts/vnodes/

    We want to have an even split of the token range so that load can be well distributed between nodes,
      as well as the ability to add new nodes and have them take a fair share of the load without the necessity
      to move data between the existing nodes
   */
  @simulacrum.typeclass
  trait Consistent[Node] extends Hashing[Node] {
    import scala.collection.JavaConverters._
    import java.util.{SortedMap ⇒ JSortedMap, TreeMap ⇒ JTreeMap}
    private val numberOfVNodes = 4
    override val seed          = 512L
    override val name          = "consistent-hashing"

    private val ring: JSortedMap[Long, Node] = new JTreeMap[Long, Node]()

    private def writeInt(arr: Array[Byte], i: Int, offset: Int): Array[Byte] = {
      arr(offset) = (i >>> 24).toByte
      arr(offset + 1) = (i >>> 16).toByte
      arr(offset + 2) = (i >>> 8).toByte
      arr(offset + 3) = i.toByte
      arr
    }

    /*private def readInt(arr: Array[Byte], offset: Int): Int = {
      (arr(offset) << 24) |
        (arr(offset + 1) & 0xff) << 16 |
        (arr(offset + 2) & 0xff) << 8 |
        (arr(offset + 3) & 0xff)
    }*/

    override def removeNode(node: Node): Boolean =
      //println(s"remove $node")
      (0 to numberOfVNodes).foldLeft(true) { (acc, vNodeId) ⇒
        val vNodeSuffix = Array.ofDim[Byte](4)
        writeInt(vNodeSuffix, vNodeId, 0)
        val bytes          = toBinary(node) ++ vNodeSuffix
        val nodeHash128bit = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(bytes), 0, bytes.length, seed)(1)
        //println(s"$node - vnode:$vNodeId")
        acc & node == ring.remove(nodeHash128bit)
      }

    override def addNode(node: Node): Boolean =
      //Hash each node several times (VNodes)
      if (validated(node)) {
        (0 to numberOfVNodes).foldLeft(true) { (acc, i) ⇒
          val suffix = Array.ofDim[Byte](4)
          writeInt(suffix, i, 0)
          val bytes          = toBinary(node) ++ suffix
          val nodeHash128bit = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(bytes), 0, bytes.length, seed)(1)
          acc & (node == ring.put(nodeHash128bit, node))
        }
      } else false

    override def nodeFor(key: String, rf: Int): Set[Node] = {
      if (rf > ring.keySet.size)
        throw new Exception("Replication factor more than the number of the ranges on a ring")

      val bytes         = key.getBytes(UTF_8)
      val keyHash128bit = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(bytes), 0, bytes.length, seed)(1)
      if (ring.containsKey(keyHash128bit)) {
        ring.keySet.asScala.take(rf).map(ring.get).to[scala.collection.immutable.Set]
      } else {
        val tailMap    = ring.tailMap(keyHash128bit)
        val candidates = tailMap.keySet.asScala.take(rf).map(ring.get).to[scala.collection.immutable.Set]
        if (candidates.size < rf) {
          //we must be at the end of the ring so we go to the first entry and so on
          candidates ++ ring.keySet.asScala.take(rf - candidates.size).map(ring.get).to[scala.collection.immutable.Set]
        } else candidates
      }
    }

    def showDiff: String = {
      val iter = ring.keySet.iterator
      val sb   = new StringBuilder

      var prevKey = Long.MinValue
      while (iter.hasNext) {
        if (prevKey == Long.MinValue)
          prevKey = iter.next
        else {
          val key  = iter.next
          val diff = key - prevKey
          sb.append(s"${key} - ${prevKey} = $diff").append("\n")
          prevKey = key
        }
      }
      sb.toString
    }

    override def toString: String = {
      val iter = ring.keySet.iterator
      val sb   = new StringBuilder
      while (iter.hasNext) {
        val key = iter.next
        sb.append(s"[${key}: ${ring.get(key)}]").append("->")
      }
      sb.toString
    }
  }

  //MinHashing alg for approximate similarity
  /*
  @simulacrum.typeclass trait MinHashing[Node] extends Hashing[Node] {
    import com.twitter.algebird.{ MinHashSignature, MinHasher32 }
    override val seed = 512l

    //hashesNum: Int = 14
    //bucketsNum: Int = 7
    //val (hashesNum, bucketsNum) = MinHasher.pickHashesAndBands(targetThreshold, maxBytes / 4)
    val maxBytes = 64
    val targetThreshold = 0.5
    val mh = new MinHasher32(targetThreshold, maxBytes)

    case class Item(n: Node, s: MinHashSignature) extends java.lang.Comparable[Item] {
      override def compareTo(that: Item): Int = n.toString.compareTo(that.n.toString)
    }

    private val members = new ConcurrentSkipListSet[Item]()

    override val name = "min-hashing"

    override def removeNode(node: Node): Boolean =
      members.remove(node)

    override def addNode(node: Node): Boolean = {
      if (validated(node)) members.add(Item(node, mh.init(node.toString)))
      else false
    }

    override def nodeFor(key: String, rf: Int): Set[Node] = {
      var distances = SortedSet.empty[(Double, Node)]((x: (Double, Node), y: (Double, Node)) ⇒ x._1.compare(y._1))
      val iter = members.iterator
      val keySignature = key.map(ch ⇒ mh.init(ch.toString)).reduce(mh.plus(_, _))

      var d: Double = 0
      while (iter.hasNext) {
        val nodeSignature = iter.next
        val dist = mh.similarity(nodeSignature.s, keySignature)
        if (dist > d)
          d = dist

        //cup a ring evenly
        if (dist == 0)
          println(s"Key: $key, dist: $dist, node -> ${nodeSignature.n}")
        distances = distances + (dist → nodeSignature.n)
      }
      distances.take(rf).map(_._2)
    }
  }*/

  object Consistent {
    implicit def instance = new Consistent[String] {
      override def toBinary(node: String): Array[Byte] = node.getBytes("UTF-8")
      override def validated(node: String): Boolean    = true
    }
  }

  object Rendezvous {
    implicit def instance = new Rendezvous[String] {
      override def toBinary(node: String): Array[Byte] = node.getBytes("UTF-8")
      override def validated(node: String): Boolean    = true
    }

    /*implicit def instance1 = new Rendezvous[Int] {
      override def toBinary(node: Int): Array[Byte] = "node".getBytes("UTF-8")
      override def validated(node: Int): Boolean = true
    }*/
  }

  /*object MinHashing {
    implicit def instance = new MinHashing[String] {
      override def toBinary(node: String): Array[Byte] = node.getBytes("utf-8")
      override def validated(node: String): Boolean = true
    }
  }*/

  object HashingRouter {

    //HashingRouter.create2[Consistent, String]
    def create2[F[_], A: ClassTag](implicit tag: ClassTag[A], hashAlg: F[A]): F[A] =
      hashAlg

    //HashingRouter.create[String, Consistent]
    def create[A: ClassTag, F[A] <: Hashing[A]](implicit tag: ClassTag[A], hashAlg: F[A]): F[A] =
      //println(tag.runtimeClass.getName)
      //println(hashAlg.getClass.getName)
      hashAlg

    //Phantom type
    def apply[F <: Hashing[_]: ClassTag](implicit tag: ClassTag[F], alg: F): Hashing[T] forSome { type T } = {
      //val alg = implicitly[A]
      println(tag.runtimeClass.getName)
      println(alg.getClass.getName)
      //val ref = implicitly[ClassTag[A]].runtimeClass.newInstance().asInstanceOf[A]
      alg
    }
  }

  //HashingRouter[Consistent[String]]

  //HashingRouter[Rendezvous[String]]

  /*
  sealed trait HashingRouter[F[_] <: Hashing[_], N] {
    def apply(nodes: util.Collection[N]): F[N]
  }
  sealed trait HashingRouter[A <: Hashing[N], N] {
    def withNodes(nodes: util.Collection[N]): A
  }
   */

  /*
  trait HashRouter {
    type T
    type F[T] <: Hashing[T]

    def alg: F[T]

    def remove(node: T): Boolean =
      alg.removeNode(node)

    def add(node: T): Boolean =
      alg.addNode(node)

    def get(key: String): T =
      alg.get(key, 1).head

    def get(key: String, rf: Int): Set[T] =
      alg.get(key, rf)
  }

  implicit val a = new HashRouter {
    override type F[T] = Rendezvous[T]
    override def alg: Rendezvous[T] = {
      implicitly[Rendezvous[T]]
    }
  }

  implicit val b = new HashRouter {
    override type F[T] = Consistent[T]
    override def alg: Consistent[T] = {
      implicitly[Consistent[T]]
    }
  }

  def routerAlg[T]()(implicit r: HashRouter): r.F[T] = {
    r.alg
    //???
  }
 */

}

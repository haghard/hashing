package hashing

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ConcurrentSkipListSet
import scala.collection.immutable.SortedSet

object HashingTypeclasses {

  @simulacrum.typeclass trait Hashing[Node] {
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

    def get(key: String, rf: Int): Set[Node]

    def validated(node: Node): Boolean
  }

  @simulacrum.typeclass trait Rendezvous[Node] extends Hashing[Node] {
    override val name = "rendezvous-hash"
    protected val hash = scala.util.hashing.MurmurHash3
    protected val members: ConcurrentSkipListSet[Node] = new ConcurrentSkipListSet[Node]()

    case class Item(hash: Int, node: Node)

    override def removeNode(node: Node): Boolean = {
      println(s"remove $node")
      members.remove(node)
    }

    override def addNode(node: Node): Boolean = {
      if (validated(node)) {
        members.add(node)
      } else false
    }

    override def get(key: String, rf: Int): Set[Node] = {
      var allHashes = SortedSet.empty[Item](new Ordering[Item]() {
        override def compare(x: Item, y: Item): Int =
          -x.hash.compare(y.hash)
      })
      val iter = members.iterator
      while (iter.hasNext) {
        val node = iter.next
        val keyBytes = key.getBytes
        val nodeBytes = toBinary(node)
        val bytes = ByteBuffer.allocate(keyBytes.length + nodeBytes.length).put(keyBytes).put(nodeBytes).array()
        val hash0 = hash.arrayHash(bytes)
        allHashes = allHashes + Item(hash0, node)
      }
      allHashes.take(rf).map(_.node)
    }
  }

  @simulacrum.typeclass trait Consistent[Node] extends Hashing[Node] {
    import java.util.{ SortedMap ⇒ JSortedMap, TreeMap ⇒ JTreeMap }

    override val name = "consistent-hash"
    protected val hash = scala.util.hashing.MurmurHash3
    protected val ring: JSortedMap[Int, Node] = new JTreeMap[Int, Node]()

    override def removeNode(node: Node): Boolean = {
      val bytes = toBinary(node)
      //val hash =  new BigInteger(1, bytes.md5.bytes).intValue()
      val hash0 = hash.arrayHash(bytes)
      println(s"remove $node - $hash0")
      node == ring.remove(hash0)
    }

    override def addNode(node: Node): Boolean = {
      if (validated(node)) {
        val bytes = toBinary(node)
        val hash0 = hash.arrayHash(bytes)
        ring.put(hash0, node)
        true
      } else false
    }

    override def get(key: String, rf: Int): Set[Node] = {
      val bytes = key.getBytes
      var hash0 = hash.arrayHash(bytes)
      if (!ring.containsKey(hash0)) {
        val tailMap = ring.tailMap(hash0)
        hash0 = if (tailMap.isEmpty) ring.firstKey else tailMap.firstKey
      }
      Set(ring.get(hash0))
    }
  }

  object Consistent {
    implicit def instance = new Consistent[String] {
      override def toBinary(node: String): Array[Byte] = node.getBytes("utf-8")
      override def validated(node: String): Boolean = true
    }
  }

  object Rendezvous {
    implicit def instance = new Rendezvous[String] {
      override def toBinary(node: String): Array[Byte] = node.getBytes("utf-8")
      override def validated(node: String): Boolean = true
    }
  }

  /*
  sealed trait HashingRouter[A <: Hashing[N], N] {
    def withNodes(nodes: Set[N]): A
  }

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

package hashing

import java.util
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListSet
import scala.collection.immutable.SortedSet

import scala.language.postfixOps

object Hashing {

  sealed trait Hashing

  trait RendezvousHashing extends Hashing

  trait ConsistentHashing extends Hashing

  trait HashingAlg[Node, H <: Hashing] {
    protected val hash = scala.util.hashing.MurmurHash3

    def initNodes(nodes: util.Collection[Node]): Unit = {
      val iter = nodes.iterator
      while (iter.hasNext) {
        addNode(iter.next)
      }
    }

    def toBinary(node: Node): Array[Byte]

    def removeNode(node: Node): Boolean

    def addNode(node: Node): Boolean

    def get(key: String, rf: Int): Set[Node]

    def validate(node: Node): Boolean
  }

  //Highest Random Weight (HRW) hashing
  //https://github.com/clohfink/RendezvousHash
  trait Rendezvous[Node, H <: Hashing] extends HashingAlg[Node, H] {
    protected val members: ConcurrentSkipListSet[Node] = new ConcurrentSkipListSet[Node]()

    case class Item(hash: Int, node: Node)

    override def removeNode(node: Node): Boolean = {
      println(s"remove $node")
      members.remove(node)
    }

    override def addNode(node: Node): Boolean = {
      if (validate(node)) {
        //println(s"add $node")
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
        //val hash =  new BigInteger(1, bytes.md5.bytes).intValue()
        val hash0 = hash.arrayHash(bytes)
        allHashes = allHashes + Item(hash0, node)
      }
      allHashes.take(rf).map(_.node)
    }
  }

  //https://infinitescript.com/2014/10/consistent-hash-ring/
  //https://community.oracle.com/blogs/tomwhite/2007/11/27/consistent-hashing
  trait ConsistentHash[Node, H <: Hashing] extends HashingAlg[Node, H] {

    import java.util.{ SortedMap ⇒ JSortedMap, TreeMap ⇒ JTreeMap }

    protected val ring: JSortedMap[Int, Node] = new JTreeMap[Int, Node]()

    override def removeNode(node: Node): Boolean = {
      val bytes = toBinary(node)
      //val hash =  new BigInteger(1, bytes.md5.bytes).intValue()
      val hash0 = hash.arrayHash(bytes)
      node == ring.remove(hash0)
    }

    override def addNode(node: Node): Boolean = {
      if (validate(node)) {
        val bytes = toBinary(node)
        //val hash =  new BigInteger(1, bytes.md5.bytes).intValue()
        val hash0 = hash.arrayHash(bytes)
        //println(s"add $node - $hash")
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

  object HashingAlg {
    implicit val chString = new ConsistentHash[String, ConsistentHashing] {
      override def toBinary(node: String): Array[Byte] = node.getBytes

      override def validate(node: String): Boolean = {
        //TODO: Pattern for IpAddress
        true
      }
    }

    implicit val rendezvousString = new Rendezvous[String, RendezvousHashing] {
      override def toBinary(node: String): Array[Byte] = node.getBytes

      override def validate(node: String): Boolean = {
        //TODO: Pattern for IpAddress
        true
      }
    }

    def apply[Node, A <: Hashing](implicit alg: HashingAlg[Node, A]): HashingAlg[Node, A] =
      alg
  }

  trait HashingRouter[Node, H <: Hashing] {
    type B <: HashingAlg[Node, H]

    protected def alg: B

    protected def build(nodes: util.Collection[Node]): HashingRouter[Node, H] = {
      alg.initNodes(nodes)
      this
    }

    def remove(node: Node): Boolean =
      alg.removeNode(node)

    def add(node: Node): Boolean =
      alg.addNode(node)

    def get(key: String): Node =
      alg.get(key, 1).head

    def get(key: String, rf: Int): Set[Node] =
      alg.get(key, rf)
  }

  object HashingRouter {
    def apply[Node, A <: Hashing](nodes: util.Collection[Node])(implicit hashingAlg: HashingAlg[Node, A]): HashingRouter[Node, A] = {
      val router = new HashingRouter[Node, A] {
        override type B = HashingAlg[Node, A]
        override val alg = hashingAlg
      }.build(nodes)

      router
    }
  }
}

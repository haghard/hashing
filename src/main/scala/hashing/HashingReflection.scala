package hashing

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ConcurrentSkipListSet
import java.util.{ SortedMap ⇒ JSortedMap, TreeMap ⇒ JTreeMap }

import scala.collection.immutable.SortedSet
import scala.reflect.ClassTag

object HashingReflection {

  trait Hashing[T] {
    protected val hash = scala.util.hashing.MurmurHash3

    def name: String

    def withNodes(nodes: util.Collection[T]): Hashing[T] = {
      val iter = nodes.iterator
      while (iter.hasNext) {
        addNode(iter.next)
      }
      this
    }

    def toBinary(node: T): Array[Byte]

    def removeNode(node: T): Boolean

    def addNode(node: T): Boolean

    def get(key: String, rf: Int): Set[T]

    def validated(node: T): Boolean
  }

  class Rendezvous extends Hashing[String] {
    override val name = "rendezvous-hash"
    protected val members: ConcurrentSkipListSet[String] = new ConcurrentSkipListSet[String]()

    case class Item(hash: Int, node: String)

    override def removeNode(node: String): Boolean = {
      println(s"remove $node")
      members.remove(node)
    }

    override def addNode(node: String): Boolean = {
      if (validated(node)) {
        members.add(node)
      } else false
    }

    override def get(key: String, rf: Int): Set[String] = {
      var allHashes = SortedSet.empty[Item]((x: Item, y: Item) => -x.hash.compare(y.hash))
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

    override def toBinary(node: String): Array[Byte] = node.getBytes

    override def validated(node: String): Boolean = true
  }

  class Consistent extends Hashing[String] {
    override val name = "consistent-hash"
    protected val ring: JSortedMap[Int, String] = new JTreeMap[Int, String]()

    override def removeNode(node: String): Boolean = {
      val bytes = toBinary(node)
      val hash0 = hash.arrayHash(bytes)
      println(s"remove $node - $hash0")
      node == ring.remove(hash0)
    }

    override def addNode(node: String): Boolean = {
      if (validated(node)) {
        val bytes = toBinary(node)
        val hash0 = hash.arrayHash(bytes)
        ring.put(hash0, node)
        true
      } else false
    }

    override def get(key: String, rf: Int): Set[String] = {
      val bytes = key.getBytes
      var hash0 = hash.arrayHash(bytes)
      if (!ring.containsKey(hash0)) {
        val tailMap = ring.tailMap(hash0)
        hash0 = if (tailMap.isEmpty) ring.firstKey else tailMap.firstKey
      }
      Set(ring.get(hash0))
    }

    override def toBinary(node: String): Array[Byte] = node.getBytes

    override def validated(node: String): Boolean = true
  }

  implicit class Routers[T](val nodes: util.Collection[T]) extends AnyVal {
    /**
     *
     *  val nodes = java.util.Arrays.asList("a", "b")
     *  nodes.router[Rendezvous]  - success
     *
     *  val nodes = java.util.Arrays.asList(1, 2)
     *  nodes.router[Rendezvous]  - compilation error
     *  error: type arguments [hashing.Rendezvous] do not conform to method router's type parameter bounds [A <: Hashing[Int]]
     *
     */
    def router[A <: Hashing[T]: ClassTag] = {
      val ref = implicitly[ClassTag[A]].runtimeClass.newInstance().asInstanceOf[A]
      ref.withNodes(nodes)
    }
  }

  //type A = Hashing forSome { type T }
}

/*
  def apply[J <: Batch[_, _]: ClassTag]() = {
    implicitly[ClassTag[J]].runtimeClass.newInstance().asInstanceOf[J]
  }
 */

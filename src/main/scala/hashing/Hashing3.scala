package hashing

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ConcurrentSkipListMap, ConcurrentSkipListSet}

import com.twitter.algebird.CassandraMurmurHash

import scala.annotation.implicitNotFound
import scala.collection.immutable.SortedSet
import java.util.{SortedMap ⇒ JSortedMap}
import scala.collection.JavaConverters._

object Hashing3 {

  trait HashTypesModule {
    type Replica
    type A <: HashAlg[Replica]
  }

  trait HashAlg[T] {
    def name: String

    def toBinary(node: T): Array[Byte]

    def remove(node: T): Boolean

    def add(node: T): Boolean

    def memberFor(key: String, rf: Int): Set[T]

    def numOfReplicas: Int
  }

  trait Consistent[T] extends HashAlg[T] {
    private val numberOfVNodes = 1 << 5
    private val seed           = 512L

    private var replicas: Set[T]          = Set.empty[T]
    private val ring: JSortedMap[Long, T] = new ConcurrentSkipListMap[Long, T]()

    private def writeInt(arr: Array[Byte], i: Int, offset: Int): Array[Byte] = {
      arr(offset) = (i >>> 24).toByte
      arr(offset + 1) = (i >>> 16).toByte
      arr(offset + 2) = (i >>> 8).toByte
      arr(offset + 3) = i.toByte
      arr
    }

    override def remove(replica: T): Boolean = {
      //println(s"remove $node")
      replicas = replicas - replica
      (0 to numberOfVNodes).foldLeft(true) { (acc, vNodeId) ⇒
        val vNodeSuffix = Array.ofDim[Byte](4)
        writeInt(vNodeSuffix, vNodeId, 0)
        val bytes          = toBinary(replica) ++ vNodeSuffix
        val nodeHash128bit = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(bytes), 0, bytes.length, seed)(1)
        //println(s"$node - vnode:$vNodeId")
        acc & replica == ring.remove(nodeHash128bit)
      }
    }

    override def add(replica: T): Boolean = {
      //Hash each node several times (VNodes)
      replicas = replicas + replica
      (0 to numberOfVNodes).foldLeft(true) { (acc, i) ⇒
        val suffix = Array.ofDim[Byte](4)
        writeInt(suffix, i, 0)
        val bytes          = toBinary(replica) ++ suffix
        val nodeHash128bit = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(bytes), 0, bytes.length, seed)(1)
        acc & (replica == ring.put(nodeHash128bit, replica))
      }
    }

    override def memberFor(key: String, rf: Int): Set[T] = {
      val localRing = ring
      if (rf > localRing.keySet.size / numberOfVNodes)
        throw new Exception("Replication factor more than the number of the ranges in the ring")

      val keyBytes = key.getBytes(UTF_8)
      val keyHash  = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(keyBytes), 0, keyBytes.length, seed)(1)

      val r             = localRing.tailMap(keyHash)
      val tillEnd       = r.values.asScala
      val fromBeginning = localRing.subMap(ring.firstKey, r.firstKey).values.asScala
      (tillEnd ++ fromBeginning).take(Math.min(rf, ring.size)).toSet

      /*
      var i    = 0
      var res  = Set.empty[T]
      val it = tail.iterator
      if (tail.size >= rf) {
        val it = tail.iterator
        while (it.hasNext && i < rf) {
          val n = it.next
          if (!res.contains(n)) {
            i += 1
            res = res + n
          }
        }
        res
      } else {
        while (it.hasNext) {
          val n = it.next
          if (!res.contains(n)) {
            i += 1
            res = res + n
          }
        }
        val it0 = all.iterator
        while (it0.hasNext && i < rf) {
          val n = it0.next
          if (!res.contains(n)) {
            i += 1
            res = res + n
          }
        }
        res
      }*/
    }

    override def numOfReplicas: Int = replicas.size

    def numOfVnodes: Int = numberOfVNodes
  }

  /**
    * Highest Random Weight (HRW) hashing
    * https://github.com/clohfink/RendezvousHash
    * https://www.pvk.ca/Blog/2017/09/24/rendezvous-hashing-my-baseline-consistent-distribution-method/
    * A random uniform way to partition your keyspace up among the available nodes
    */
  trait Rendezvous[T] extends HashAlg[T] {
    private val seed = 512L
    private val ring = new ConcurrentSkipListSet[T]()

    override def remove(replica: T): Boolean =
      ring.remove(replica)

    override def add(replica: T): Boolean =
      ring.add(replica)

    override def memberFor(key: String, rf: Int): Set[T] = {
      var candidates = SortedSet.empty[(Long, T)]((x: (Long, T), y: (Long, T)) ⇒ -x._1.compare(y._1))
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

    override def numOfReplicas: Int = ring.size
  }

  trait ConsistentForStrings extends HashTypesModule {
    override type Replica = String
    override type A       = Consistent[Replica]
  }

  trait ConsistentForLongs extends HashTypesModule {
    override type Replica = Long
    override type A       = Consistent[Replica]
  }

  trait RendezvousForStrings extends HashTypesModule {
    override type Replica = String
    override type A       = Rendezvous[Replica]
  }

  @implicitNotFound(msg = "Cannot find HashBuilder type class instance for ${T}")
  trait HashBuilder[T <: HashTypesModule] {
    def replicas(replicas: Set[T#Replica]): T#A
  }

  object HashBuilder {

    implicit object a extends HashBuilder[ConsistentForStrings] {
      override def replicas(replicas: Set[String]): Consistent[String] = new Consistent[String] {

        replicas.foreach(add(_))

        override val name: String = "consistent-hashing for string"

        override def toBinary(node: String): Array[Byte] =
          node.getBytes(UTF_8)
      }
    }

    implicit object b extends HashBuilder[ConsistentForLongs] {
      override def replicas(replicas: Set[Long]): Consistent[Long] = new Consistent[Long] {

        replicas.foreach(add(_))

        override val name: String = "consistent-hashing for longs"

        override def toBinary(node: Long): Array[Byte] =
          node.toHexString.getBytes(UTF_8)
      }
    }

    implicit object c extends HashBuilder[RendezvousForStrings] {
      override def replicas(replicas: Set[String]): Rendezvous[String] = new Rendezvous[String] {

        replicas.foreach(add(_))

        override val name: String = "rendezvous for string"
        override def toBinary(node: String): Array[Byte] =
          node.getBytes(UTF_8)
      }
    }

    def apply[T <: HashTypesModule: HashBuilder]: HashBuilder[T] =
      implicitly[HashBuilder[T]]
  }

  HashBuilder[ConsistentForStrings].replicas(Set("a", "b", "c", "d"))

  val alg = HashBuilder[RendezvousForStrings].replicas(Set("a", "b", "c", "d"))
  alg.add("a")
  alg.add("b")
  alg.add("c")
  alg.add("d")
}

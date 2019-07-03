package hashing

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentSkipListSet

import com.twitter.algebird.CassandraMurmurHash

import scala.annotation.implicitNotFound
import scala.collection.immutable.SortedSet
import scala.collection.JavaConverters._
import java.util.{SortedMap ⇒ JSortedMap, TreeMap ⇒ JTreeMap}

object Hashing3 {

  trait HashTypesModule {
    type Replica
    type A <: HashAlg[Replica]
  }

  trait HashAlg[T] {
    def name: String

    def toBinary(node: T): Array[Byte]

    def removeReplica(node: T): Boolean

    def addReplica(node: T): Boolean

    def replicaFor(key: String, rf: Int): Set[T]
  }

  trait Consistent[T] extends HashAlg[T] {

    private val numberOfVNodes = 4
    private val seed           = 512L

    private var replicas: Set[T]          = Set.empty[T]
    private val ring: JSortedMap[Long, T] = new JTreeMap[Long, T]()

    private def writeInt(arr: Array[Byte], i: Int, offset: Int): Array[Byte] = {
      arr(offset) = (i >>> 24).toByte
      arr(offset + 1) = (i >>> 16).toByte
      arr(offset + 2) = (i >>> 8).toByte
      arr(offset + 3) = i.toByte
      arr
    }

    override def removeReplica(replica: T): Boolean = {
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

    override def addReplica(replica: T): Boolean = {
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

    override def replicaFor(key: String, rf: Int): Set[T] = {
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

    def numOfVnodes: Int = numberOfVNodes

    def numOfReplicas: Int = replicas.size
  }

  trait Rendezvous[T] extends HashAlg[T] {
    private val seed = 512L
    private val ring = new ConcurrentSkipListSet[T]()

    override def removeReplica(replica: T): Boolean =
      ring.remove(replica)

    override def addReplica(replica: T): Boolean =
      ring.add(replica)

    override def replicaFor(key: String, rf: Int): Set[T] = {
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

    def numOfReplicas: Int = ring.size()
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

  @implicitNotFound(msg = "Cannot find Hash type class instance for ${T}")
  trait Hash[T <: HashTypesModule] {
    def alg: T#A
  }

  object Hash {

    implicit object a extends Hash[ConsistentForStrings] {
      override val alg: Consistent[String] = new Consistent[String] {
        override val name: String = "consistent-hashing for string"

        override def toBinary(node: String): Array[Byte] =
          node.getBytes(UTF_8)
      }
    }

    implicit object b extends Hash[ConsistentForLongs] {
      override val alg: Consistent[Long] = new Consistent[Long] {
        override val name: String = "consistent-hashing for longs"

        override def toBinary(node: Long): Array[Byte] =
          node.toHexString.getBytes(UTF_8)
      }
    }

    implicit object c extends Hash[RendezvousForStrings] {
      override val alg: Rendezvous[String] = new Rendezvous[String] {
        override val name: String = "rendezvous for longs"
        override def toBinary(node: String): Array[Byte] =
          node.getBytes(UTF_8)
      }
    }

    def apply[T <: HashTypesModule: Hash]: Hash[T] =
      implicitly[Hash[T]]
  }

  Hash[ConsistentForStrings].alg

  val alg = Hash[RendezvousForStrings].alg
  alg.addReplica("a")
  alg.addReplica("b")
  alg.addReplica("c")
}
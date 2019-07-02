package hashing

object Ring {
  type PartitionId = Int
  case class NodeId(id: Int)

  /**
    * Recommended by Riak nodesSize = 5, partitionsSize = 64
    *
    * @param nodesSize - nr of initial cluster size of nodes
    * @param partitionsSize - nr of partitions Ring consists of
    * @return representation of Ring
    */
  def apply(nodesSize: Int, partitionsSize: Int): Ring =
    new Ring((0 until nodesSize).flatMap { id ⇒
      (id until partitionsSize by nodesSize).map((_, NodeId(id)))
    }.toMap)
}

class Ring(private val ring: Map[Ring.PartitionId, Ring.NodeId]) {
  import Ring._

  def getNodeId(id: PartitionId): Option[NodeId] = ring.get(id)

  def size: Int = ring.size

  def nodesId: Set[NodeId] = ring.values.toSet

  def swap: Map[NodeId, List[PartitionId]] =
    ring.groupBy(_._2).mapValues(_.keys.toList.sorted)

  def nextPartitionId(id: PartitionId): PartitionId =
    (id + 1) % ring.size
}

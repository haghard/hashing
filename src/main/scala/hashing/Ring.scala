package hashing

object Ring {
  type PartitionId = Int
  case class NodeId(id: Int)

  def apply(nodesSize: Int, partitionsSize: Int): Ring = {
    val partitions2Nodes =
      (0 until nodesSize).flatMap { id =>
        (id until partitionsSize by nodesSize).map((_, NodeId(id)))
      }
    new Ring(partitions2Nodes.toMap)
  }
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
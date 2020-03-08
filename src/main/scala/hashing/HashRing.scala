package hashing

import scala.collection.immutable.SortedMap

case class HashRing(private val ring: SortedMap[Long, String], start: Long, end: Long, step: Long) {

  /**
    * Alias for [[add]] method
    */
  def :+(node: String): Option[(HashRing, Set[(Long, String)])] =
    add(node)

  /**
    *
    * Adds a node to the node ring.
    * Note that the instance is immutable and this operation returns a new instance.
    *
    * When we add new node, it changes the ownership of some ranges by splitting it up.
    */
  def add(node: String): Option[(HashRing, Set[(Long, String)])] =
    if (nodes.contains(node))
      None
    else {
      // this could be improved e.g. we should rely on least taken resources
      val ringStep = nodes.size + 1
      val takeOvers = ((step * ringStep) until end by (step * ringStep))
        .map(pId ⇒ (pId, lookup(pId).head))
        .toSet

      val updatedRing = takeOvers.foldLeft(ring) {
        case (acc, (pId, _)) ⇒ acc.updated(pId, node)
      }

      Option(HashRing(updatedRing, start, end, step) → takeOvers)
    }

  /**
    * Alias for [[remove]] method
    */
  def :-(node: String): Option[HashRing] =
    remove(node)

  def remove(node: String): Option[HashRing] =
    if (!nodes.contains(node))
      None
    else {
      val m = ranges(node) match {
        case Nil ⇒ ring
        case list ⇒
          if (list.size == 1) ring - list(0)
          else if (list.size == 2) ring - (list(0), list(1))
          else if (list.size == 3) ring - (list(0), list(1), list(2))
          else ring - (list(0), list(1), list.splitAt(2)._2: _*)
      }

      Some(HashRing(m, start, end, step))
    }

  def lookup(hash: Long, rf: Int = 1): Vector[String] =
    (ring.keysIteratorFrom(hash) ++ ring.keysIteratorFrom(ring.firstKey))
      .take(Math.min(rf, nodes.size))
      .map(ring(_))
      .toVector

  def last: Long = ring.lastKey

  def first: Long = ring.firstKey

  def size: Int = ring.size

  def nodes: Set[String] = ring.values.toSet

  def ranges: Map[String, List[Long]] =
    ring.groupBy(_._2).mapValues(_.keys.toList.sorted)

  def showSubRange(startKey: Long, endKey: Long): String = {
    var cur  = startKey
    val sb   = new StringBuilder
    val iter = ring.keysIteratorFrom(startKey)
    while (iter.hasNext && cur <= endKey) {
      val key = iter.next
      cur = key
      sb.append(s"[${key}: ${ring(key)}]").append("->")
    }
    sb.toString
  }

  override def toString: String = {
    val sb   = new StringBuilder
    val iter = ring.keysIteratorFrom(ring.firstKey)
    while (iter.hasNext) {
      val key = iter.next
      sb.append(s"[${ring(key)}:${key}]").append("->")
    }
    sb.toString
  }
}

object HashRing {

  def apply(
    name: String,
    start: Long = Long.MinValue,
    end: Long = Long.MaxValue,
    step: Long = 6917529027641080L //2667 partitions, change  if you need more
  ): HashRing =
    HashRing(
      (start until end by step)
        .foldLeft(SortedMap[Long, String]())((acc, c) ⇒ acc + (c → name)),
      start,
      end,
      step
    )
}

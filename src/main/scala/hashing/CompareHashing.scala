package hashing

import java.util
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger

import brave.Tracing
import zipkin.reporter.AsyncReporter
import zipkin.reporter.okhttp3.OkHttpSender

/**
 * Using a tracer, you can create a root span capturing the critical path of a request. Child
 * spans can be created to allocate latency relating to outgoing requests.
 *
 * Here's a contrived example:
 * <pre>{@code
 * Span twoPhase = tracer.newTrace().name("twoPhase").start();
 * try {
 *   Span prepare = tracer.newChild(twoPhase.context()).name("prepare").start();
 *   try {
 *     prepare();
 *   } finally {
 *     prepare.finish();
 * }
 * Span commit = tracer.newChild(twoPhase.context()).name("commit").start();
 * try {
 * commit();
 * } finally {
 *     commit.finish();
 * }
 * } finally {
 *   twoPhase.finish();
 * }
 * }</pre>
 *
 */

import hashing.Hashing.{ ConsistentHashing, HashingRouter, RendezvousHashing }

//https://github.com/lloydmeta/zipkin-futures/blob/master/zipkin-futures-core/src/test/scala/com/beachape/zipkin/services/ZipkinServiceSpec.scala
//https://github.com/openzipkin/brave/blob/master/brave/README.md
//https://github.com/openzipkin/zipkin-finagle-example

//sbt runMain hashing.CompareHashing

//Depricated. please use CompareHashingTC
object CompareHashing {
  val keysNum = 500000

  val sender = OkHttpSender.create("http://...:9411/api/v1/spans")
  val reporter = AsyncReporter.builder(sender).build()
  val tracing = Tracing.newBuilder().localServiceName("hashing")
    .reporter(reporter)
    .build()

  val tracer = tracing.tracer()

  val nodes0 = "alpha-1" :: "beta-2" :: "gamma-3" :: "delta-4" :: "epsilon-5" :: "zeta-6" :: "eta-7" ::
    "theta-8" :: "iota-9" :: "kappa-10" :: Nil

  private def getNodes(distribution: util.Map[String, AtomicInteger]) = {
    val nodes = new java.util.ArrayList[String]
    (0 until nodes0.size).foreach { i ⇒
      val node = nodes0(i)
      nodes.add(node)
      distribution.put(node, new AtomicInteger)
    }
    nodes
  }

  def iter(router: HashingRouter[String, _], distribution: Map[String, AtomicInteger]) = {
    (0 until keysNum).foreach { i ⇒
      distribution.get(router.nodeFor(s"my-long-key-$i", 1)).incrementAndGet()
    }

    val iter = distribution.entrySet().iterator()
    while (iter.hasNext) {
      val e = iter.next
      println(e.getKey + ", " + e.getValue.get)
      e.getValue.set(0)
    }

    println("====== remove ========")

    (0 until 4).foreach { i ⇒
      val node = nodes0(i)
      router.remove(node)
      distribution.remove(node)
    }

    (0 until keysNum).foreach { i ⇒
      distribution.get(router.nodeFor(s"my-long-key-$i", 1)).incrementAndGet()
    }

    println("====== stats ========")
    val iter1 = distribution.entrySet.iterator
    while (iter1.hasNext) {
      val e = iter1.next
      println(e.getKey + ", " + e.getValue.get)
    }
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis
    println("======: ConsistentHash :========")

    def run = {
      val rootSpan = tracer.newTrace().name("hash").start()

      val distribution0: Map[String, AtomicInteger] = new util.HashMap[String, AtomicInteger]()
      val span0 = tracer.newChild(rootSpan.context).name("consistent").start()
      val ctx = span0.context()
      println(s"TraceId: ${ctx.traceIdString}  SpanId:${ctx.spanId}")
      iter(HashingRouter[String, ConsistentHashing](getNodes(distribution0)), distribution0)
      span0.finish()

      println("======: RendezvousHashing :========")
      val distribution1: Map[String, AtomicInteger] = new util.HashMap[String, AtomicInteger]()
      val span1 = tracer.newChild(rootSpan.context).name("rendezvous").start()
      val ctx1 = span1.context
      println(s"TraceId: ${ctx1.traceIdString}  SpanId:${ctx1.spanId}")
      iter(HashingRouter[String, RendezvousHashing](getNodes(distribution1)), distribution1)
      span1.finish()
      rootSpan.finish()

      val latency = System.currentTimeMillis - start
      println("==============")
      println("Latency: " + latency)
    }

    (0 until 5).foreach(_ ⇒ run)

    tracing.close
    reporter.close
    sender.close
  }
}

/*
    val vnodes = 5
    val replicas = Cache("alpha") :: Cache("beta") :: Cache("gamma") :: Cache("delta") ::
      Cache("epsilon") :: Cache("zeta") :: Cache("eta") ::
      Cache("theta") :: Cache("iota") :: Cache("kappa") :: Nil

    val ch = ConsistentHash[String, Cache](new MD5HashFunction, vnodes, replicas)

    ch.get(Some("key-a"))
    ch.get(Some("key-a"))
*/ 
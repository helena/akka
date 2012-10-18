/*
 * Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing

import language.implicitConversions
import language.postfixOps
import akka.actor._
import akka.cluster._
import akka.routing._
import akka.dispatch.Dispatchers
import akka.event.Logging
import java.util.concurrent.atomic.AtomicReference
import akka.routing.Destination
import akka.cluster.NodeMetrics
import akka.routing.Broadcast
import akka.actor.OneForOneStrategy
import akka.cluster.ClusterEvent.ClusterMetricsChanged
import akka.cluster.ClusterEvent.CurrentClusterState
import util.Try
import concurrent.forkjoin.ThreadLocalRandom
import akka.cluster.NodeMetrics.{ HeapMemory, NodeMetricsComparator, MetricValues }
import NodeMetricsComparator._

/**
 * INTERNAL API.
 *
 * Trait that embodies the contract for all load balancing implementations.
 *
 * @author Helena Edelson
 */
private[cluster] trait LoadBalancer {

  /**
   * Compares only those nodes that are deemed 'available' by the
   * [[akka.cluster.routing.ClusterRouteeProvider]]
   */
  def selectRoutee(availableNodes: Set[NodeMetrics]): Option[Address]

}

/**
 * INTERNAL API
 */
private[cluster] object ClusterLoadBalancingRouter {
  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }
}

/**
 * INTERNAL API.
 *
 * The abstract consumer of [[akka.cluster.ClusterMetricsChanged]] events and the primary consumer
 * of cluster metric data. This strategy is a metrics-aware router which performs load balancing of
 * cluster nodes with a fallback strategy of a [[akka.routing.RandomRouter]].
 *
 * Load balancing of nodes is based on .. etc etc desc forthcoming
 *
 * @author Helena Edelson
 */
trait ClusterAdaptiveLoadBalancingRouterLike extends LoadBalancer { this: RouterConfig ⇒

  def nrOfInstances: Int

  def routees: Iterable[String]

  def routerDispatcher: String

  override def createRoute(routeeProvider: RouteeProvider): Route = {
    if (resizer.isEmpty) {
      if (routees.isEmpty) routeeProvider.createRoutees(nrOfInstances)
      else routeeProvider.registerRouteesFor(routees)
    }

    val log = Logging(routeeProvider.context.system, routeeProvider.context.self)

    /**
     * The latest metric values with their statistical data.
     */
    val nodeMetrics = new AtomicReference[Set[NodeMetrics]](Set.empty)

    val metricsListener = routeeProvider.context.actorOf(Props(new Actor {
      def receive = {
        case ClusterMetricsChanged(metrics) ⇒ receiveMetrics(metrics)
        case _: CurrentClusterState         ⇒ // ignore
      }
      def receiveMetrics(metrics: Set[NodeMetrics]): Unit = {
        val crp = routeeProvider.asInstanceOf[ClusterRouteeProvider]
        log.debug("Updating node metrics by available nodes with [{}]", crp)
        val updated: Set[NodeMetrics] = nodeMetrics.get.collect { case node if crp.availbleNodes contains node.address ⇒ node }
        nodeMetrics.set(updated)
      }
      override def postStop(): Unit = Cluster(routeeProvider.context.system) unsubscribe self
    }).withDispatcher(routerDispatcher), name = "metricsListener")
    Cluster(routeeProvider.context.system).subscribe(metricsListener, classOf[ClusterMetricsChanged])

    def fallbackTo(routees: IndexedSeq[ActorRef]): ActorRef = routees(ThreadLocalRandom.current.nextInt(routees.size))

    def routeTo(): ActorRef = {
      val routees = routeeProvider.routees
      if (routees.isEmpty) routeeProvider.context.system.deadLetters
      else selectRoutee(nodeMetrics.get) match {
        case Some(address) ⇒ routees.collectFirst { case r if r.path.address == address ⇒ r } getOrElse fallbackTo(routees)
        case None          ⇒ fallbackTo(routees)
      }
    }

    {
      case (sender, message) ⇒
        message match {
          case Broadcast(msg) ⇒ toAll(sender, routeeProvider.routees)
          case msg            ⇒ List(Destination(sender, routeTo()))
        }
    }
  }
}

/**
 * TODO desc.
 *
 * @author Helena Edelson
 */
private[cluster] trait MetricsAwareClusterNodeSelector {
  import NodeMetrics.NodeMetricsComparator.longMinAddressOrdering

  /**
   * Returns the address of the available node with the lowest cumulative difference
   * between heap memory max and used/committed.
   */
  def selectByMemory(nodes: Set[NodeMetrics]): Option[Address] = Try(Some(nodes.map {
    n ⇒
      val (used, committed, max) = MetricValues.unapply(n.heapMemory)
      (n.address, max match {
        case Some(m) ⇒ ((committed - used) + (m - used) + (m - committed))
        case None    ⇒ committed - used
      })
  }.min._1)) getOrElse None

  def selectByNetworkLatency(nodes: Set[NodeMetrics]): Option[Address] = None
  /* Try(nodes.map {
      n ⇒
        val (rxBytes, txBytes) = MetricValues.unapply(n.networkLatency).get
        (n.address, (rxBytes + txBytes))
    }.min._1) getOrElse None // TODO: min or max
  */

  def selectByCpu(nodes: Set[NodeMetrics]): Option[Address] = None
  /* Try(nodes.map {
      n ⇒
        val (loadAverage, processors, combinedCpu, cores) = MetricValues.unapply(n.cpu)
        var cumulativeDifference = 0
        // TODO: calculate
        combinedCpu.get
        cores.get
        (n.address, cumulativeDifference)
    }.min._1) getOrElse None  // TODO min or max
  }*/

}

/**
 * Selects by all monitored metric types (memory, network latency, cpu...) and
 * chooses the healthiest node to route to.
 *
 * @author Helena Edelson
 */
@SerialVersionUID(1L)
private[cluster] case class ClusterAdaptiveMetricsLoadBalancingRouter(nrOfInstances: Int = 0, routees: Iterable[String] = Nil,
                                                                      override val resizer: Option[Resizer] = None,
                                                                      val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                                                                      val supervisorStrategy: SupervisorStrategy = ClusterLoadBalancingRouter.defaultSupervisorStrategy)
  extends RouterConfig with ClusterAdaptiveLoadBalancingRouterLike with MetricsAwareClusterNodeSelector {

  def selectRoutee(nodes: Set[NodeMetrics]): Option[Address] = {
    val s = Set(selectByMemory(nodes), selectByNetworkLatency(nodes), selectByCpu(nodes))
    s.head // TODO select the Address that appears with the highest or lowest frequency
  }
}

/**
 * TODO desc.
 *
 * @author Helena Edelson
 */
@SerialVersionUID(1L)
private[cluster] case class ClusterAdaptiveMemoryLoadBalancingRouter(nrOfInstances: Int = 0, routees: Iterable[String] = Nil,
                                                                     override val resizer: Option[Resizer] = None,
                                                                     val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                                                                     val supervisorStrategy: SupervisorStrategy = ClusterLoadBalancingRouter.defaultSupervisorStrategy)
  extends RouterConfig with ClusterAdaptiveLoadBalancingRouterLike with MetricsAwareClusterNodeSelector {

  def selectRoutee(nodes: Set[NodeMetrics]): Option[Address] = selectByMemory(nodes)
}

/**
 * TODO desc.
 *
 * @author Helena Edelson
 */
@SerialVersionUID(1L)
private[cluster] case class CpuLoadBalancer(nrOfInstances: Int = 0, routees: Iterable[String] = Nil,
                                            override val resizer: Option[Resizer] = None,
                                            val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                                            val supervisorStrategy: SupervisorStrategy = ClusterLoadBalancingRouter.defaultSupervisorStrategy)
  extends RouterConfig with ClusterAdaptiveLoadBalancingRouterLike with MetricsAwareClusterNodeSelector {

  def selectRoutee(nodes: Set[NodeMetrics]): Option[Address] = None
}
/**
 * TODO desc.
 *
 * @author Helena Edelson
 */
@SerialVersionUID(1L)
private[cluster] case class LoadAverageLoadBalancer(nrOfInstances: Int = 0, routees: Iterable[String] = Nil,
                                                    override val resizer: Option[Resizer] = None,
                                                    val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                                                    val supervisorStrategy: SupervisorStrategy = ClusterLoadBalancingRouter.defaultSupervisorStrategy)
  extends RouterConfig with ClusterAdaptiveLoadBalancingRouterLike with MetricsAwareClusterNodeSelector {

  def selectRoutee(nodes: Set[NodeMetrics]): Option[Address] = None
}

/**
 * TODO desc.
 *
 * @author Helena Edelson
 */
@SerialVersionUID(1L)
private[cluster] case class NetworkLatencyLoadBalancer(nrOfInstances: Int = 0, routees: Iterable[String] = Nil,
                                                       override val resizer: Option[Resizer] = None,
                                                       val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                                                       val supervisorStrategy: SupervisorStrategy = ClusterLoadBalancingRouter.defaultSupervisorStrategy)
  extends RouterConfig with ClusterAdaptiveLoadBalancingRouterLike with MetricsAwareClusterNodeSelector {

  def selectRoutee(nodes: Set[NodeMetrics]): Option[Address] = None
}

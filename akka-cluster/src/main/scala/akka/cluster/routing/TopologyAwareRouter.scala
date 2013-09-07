/*
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster.routing

import scala.collection.immutable
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor._
import akka.cluster.Cluster
import akka.event.Logging
import akka.routing._
import akka.routing.{ Broadcast, Destination }
import akka.cluster.ClusterEvent.{ ClusterTopologyChanged, CurrentClusterState }
import akka.dispatch.Dispatchers
import akka.cluster.ClusterNetworkTopology._

object TopologyAwareRouter {
  private val escalateStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }
}

@SerialVersionUID(1L)
case class TopologyAwareRouter(
  selector: ProximalSelector = NearestNeighborSelector,
  nrOfInstances: Int = 0, routees: immutable.Iterable[String] = Nil,
  override val resizer: Option[Resizer] = None,
  val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
  val supervisorStrategy: SupervisorStrategy = TopologyAwareRouter.escalateStrategy)
  extends RouterConfig with TopologyAwareRouterLike with OverrideUnsetConfig[TopologyAwareRouter] {

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case _: FromConfig | _: NoRouter | _: TopologyAwareRouter ⇒ this.overrideUnsetConfig(other)
    case _ ⇒ throw new IllegalArgumentException(s"Expected TopologyAwareRouter but found ${other}")
  }

  /**
   * Java API for setting the supervisor strategy to be used for the “head”
   * Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): TopologyAwareRouter =
    copy(supervisorStrategy = strategy)
  /**
   * Java API for setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): TopologyAwareRouter = copy(resizer = Some(resizer))

}

trait TopologyAwareRouterLike { this: RouterConfig ⇒

  /**
   * Proximity is calculated by the selector via the `topologyListener`.
   */
  def selector: ProximalSelector

  def nrOfInstances: Int

  def routees: immutable.Iterable[String]

  def routerDispatcher: String

  override def createRoute(routeeProvider: RouteeProvider): Route = {
    if (resizer.isEmpty) {
      if (routees.isEmpty) routeeProvider.createRoutees(nrOfInstances)
      else routeeProvider.registerRouteesFor(routees)
    }

    val log = Logging(routeeProvider.context.system, routeeProvider.context.self)

    /**
     * The current proximal routees, if any.
     */
    @volatile var proximalRoutees: Option[ProximalRoutees] = None

    /**
     * While proximity is only updated by the topologyListener, access occurs from the threads of the senders.
     */
    val topologyListener = routeeProvider.context.actorOf(Props(new Actor {

      val cluster = Cluster(context.system)

      override def preStart(): Unit = cluster.subscribe(self, classOf[ClusterTopologyChanged])

      override def postStop(): Unit = cluster.unsubscribe(self)

      def receive = {
        case ClusterTopologyChanged(topology) ⇒ receiveTopology(topology)
        case _: CurrentClusterState           ⇒ // ignore
      }

      def receiveTopology(topology: Topology): Unit =
        proximalRoutees = Some(new ProximalRoutees(cluster.selfAddress, routeeProvider.routees, selector.proximity(topology)))

    }).withDispatcher(routerDispatcher).withDeploy(Deploy.local), "topologyListener")

    def getNearest: ActorRef = proximalRoutees match {
      case Some(nearest) if nearest.isEmpty ⇒
        if (nearest.isEmpty) routeeProvider.context.system.deadLetters
        else nearest(ThreadLocalRandom.current.nextInt(nearest.total) + 1)
      case _ ⇒
        val currentRoutees = routeeProvider.routees
        if (currentRoutees.isEmpty) routeeProvider.context.system.deadLetters
        else currentRoutees(ThreadLocalRandom.current.nextInt(currentRoutees.size))
    }

    {
      case (sender, message) ⇒
        message match {
          case Broadcast(msg) ⇒ toAll(sender, routeeProvider.routees)
          case msg            ⇒ List(Destination(sender, getNearest))
        }
    }
  }
}

trait ProximalSelector extends Serializable {

  /**
   * The proximity per address by zone, based on the network topology.
   */
  def proximity(topology: Topology): Map[Address, Int]
}

@SerialVersionUID(1L)
case object NearestNeighborSelector extends ProximalSelector {

  def proximity(topology: Topology): Map[Address, Int] = {
    // TODO update nearest
    Map.empty
  }
}

/**
 * INTERNAL API
 *
 * Select a routee based on its proximity (nearest neighbors) in the network topology. Lower proximity, lower latency.
 */
private[cluster] class ProximalRoutees(selfAddress: Address, refs: immutable.IndexedSeq[ActorRef], proximities: Map[Address, Int]) {

  def isEmpty: Boolean = proximities.isEmpty

  def total: Int = 1 // TODO

  /**
   * Pick the appropriate routee.
   */
  def apply(value: Int): ActorRef = refs.head // TODO
}
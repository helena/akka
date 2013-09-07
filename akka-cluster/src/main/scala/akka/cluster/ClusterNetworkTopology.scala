/*
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster

import java.lang.System.{ currentTimeMillis ⇒ newTimestamp }

import language.postfixOps
import scala.util.Try
import scala.collection.immutable
import scala.collection.JavaConverters._
import akka.actor._
import com.typesafe.config._

/**
 * INTERNAL API.
 */
private[cluster] class ClusterNetworkTopology(publisher: ActorRef) extends Actor with ActorLogging {

  import akka.cluster.ClusterNetworkTopology._
  import ClusterEvent._

  val cluster = Cluster(context.system)

  import cluster.InfoLogger._

  private val config = cluster.settings.TopologyAvailabilityZones

  val topology = Topology(config) // var when we can detect changes, i.e. responding to network changes: add gossip

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    logInfo("NetworkTopology has started successfully with {} regions and {} valid availability dataCenters", topology.regions.size, topology.dataCenters.size)
  }

  override def postStop(): Unit = cluster unsubscribe self

  def receive = {
    case state: CurrentClusterState ⇒ receiveState(state)
    case _: MemberEvent             ⇒
  }

  def receiveState(state: CurrentClusterState): Unit = {
    // TODO
  }
}

/**
 * Description intentionally leaves out the notion of 'Region' in the network hierarchy.
 */
private[cluster] object ClusterNetworkTopology {

  @SerialVersionUID(1L)
  trait Graph
  case class Nearest(edge: Int) extends Graph
  case class Instance(host: String) extends Graph

  /**
   * Defines a data center within a Region.
   *
   * @param name the data center name
   *
   * @param instances the instances within the data center - TODO this would vary over time
   *
   * @param edges the nearest neighbors
   *
   * @param timestamp TODO UTC
   *
   */
  case class DataCenter(id: Int, name: String, edges: immutable.Set[Nearest], instances: immutable.Set[Instance], timestamp: Long) extends Graph

  object DataCenter {
    def apply(id: Int, name: String, edges: immutable.Set[Nearest], instances: immutable.Set[Instance]): DataCenter =
      DataCenter(id, name, edges, instances, newTimestamp)
  }

  /**
   * Light weight partition in the topology describing clusters of data centers.
   */
  case class Region(name: String, dataCenters: immutable.Set[DataCenter] = Set.empty) extends Graph

  case class Topology(regions: immutable.Set[Region]) extends Graph {

    def dataCenters: Set[DataCenter] = regions flatMap (_.dataCenters)
  }

  object Topology {

    def apply(config: Config): Topology = {
      val regions = config.root.asScala.flatMap {
        case (key, value: ConfigObject) ⇒ parseConfig(key, value.toConfig)
        case _                          ⇒ None
      }.toSet

      Topology(regions)
    }

    /**
     * Partitions proximal dataCenters per region.
     */
    def parseConfig(region: String, config: Config): Option[Region] = {
      val dataCenters = config.root.asScala.flatMap {
        case (dataCenterName, deployed: ConfigObject) ⇒ Try {
          val dc = deployed.toConfig
          val id = dc.getInt("zone-id")
          val edges = dc.getIntList("proximal-to").asScala.map(Nearest(_)).toSet
          val instances = dc.getStringList("instances").asScala.collect { case host if host.nonEmpty ⇒ Instance(host) }.toSet
          DataCenter(id, dataCenterName, edges, instances)
        }.toOption filter (_.instances.nonEmpty)
        case _ ⇒ None
      }
      if (dataCenters.nonEmpty) Some(Region(region, dataCenters.toSet)) else None
    }
  }
}

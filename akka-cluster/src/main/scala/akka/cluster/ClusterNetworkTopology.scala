/*
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster

import java.lang.System.{ currentTimeMillis ⇒ newTimestamp }

import scala.collection.immutable
import scala.collection.JavaConverters._
import akka.actor.{ Address, Actor, ActorRef, ActorLogging }
import com.typesafe.config.{ ConfigValue, ConfigList, Config, ConfigObject }

/**
 * INTERNAL API.
 */
private[cluster] class ClusterNetworkTopology(publisher: ActorRef) extends Actor with ActorLogging {

  import akka.cluster.ClusterNetworkTopology._
  import ClusterEvent._

  val cluster = Cluster(context.system)

  import cluster.InfoLogger._

  private val config = cluster.settings.TopologyAvailabilityZones

  val topology = NetworkTopology(config) // var when we can detect changes, i.e. responding to network changes: add gossip

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    logInfo("NetworkTopology has started successfully with {} regions and {} valid availability zones", topology.regions.size, topology.zones.size)
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
  trait Topology
  case class Instance(host: String) extends Topology

  /**
   * Defines a data center within a Region.
   *
   * @param name the availability zone name
   *
   * @param region the region, for example eu-west-1
   *
   * @param instances the instances within the zone - TODO this would vary over time
   *
   * @param proximalTo the nearest neighbors
   *
   * @param timestamp TODO UTC
   *
   */
  case class AvailabilityZone(name: String, region: String, instances: immutable.Set[Instance], proximalTo: immutable.Set[String], timestamp: Long) extends Topology

  object AvailabilityZone {

    def apply(name: String, region: String, instances: immutable.Set[Instance]): AvailabilityZone =
      AvailabilityZone(name, region, instances, Set.empty, newTimestamp)
  }

  case class Region(name: String, zones: immutable.Set[AvailabilityZone] = Set.empty)

  case class NetworkTopology(regions: immutable.Set[Region]) extends Topology {
    def zones: Set[AvailabilityZone] = regions.flatMap(_.zones)
  }

  object NetworkTopology {

    def apply(config: Config): NetworkTopology = {
      val regions = config.root.asScala.map {
        case (key, value: ConfigObject) ⇒ parseConfig(key, value.toConfig)
        case _                          ⇒ Region("", Set.empty)
      }.toSet

      NetworkTopology(regions)
    }

    /**
     * Parses proximal availability zones per zone, in the current region.
     */
    def parseConfig(region: String, config: Config): Region = {
      var proximals: Set[String] = Set.empty

      val zones = config.root.asScala.collect {
        case (az, deployed: ConfigObject) ⇒
          proximals += az
          val instances = deployed.asScala.collect {
            case (_, nodes: ConfigList) ⇒
              nodes.asScala.collect { case host if isValid(host) ⇒ Instance(stripQuotes(host)) }
          }.flatten

          AvailabilityZone(az, region, instances.toSet)
      }.toSet filter (_.instances.nonEmpty) map (zone ⇒ zone.copy(proximalTo = proximals filter (_ != zone.name)))

      Region(region, zones)
    }

    def isValid(value: ConfigValue): Boolean = stripQuotes(value).nonEmpty

    private def stripQuotes(value: ConfigValue): String = {
      val s = value.render
      s.substring(1, s.length - 1)
    }
  }
}

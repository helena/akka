package akka.cluster

import akka.testkit.AkkaSpec
import com.typesafe.config._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class NetworkTopologyConfigSpec extends AkkaSpec {

  import ClusterNetworkTopology._
  import NetworkTopologyConfigSpec._

  "NetworkTopology" must {
    "parse valid availability zones" in {
      val settings = new ClusterSettings(availabilityZones.withFallback(system.settings.config), system.name)
      import settings._
      val topology = NetworkTopology(TopologyAvailabilityZones)
      topology.regions.size must be(2)
      topology.regions.forall(_.zones.size == 4) must be(true)
      topology.zones.size must be(8)
      topology.zones.forall(_.instances.size == 3) must be(true)

      /*TODO remove
      topology.regions map { region ⇒
        region.zones map { az ⇒
          println("\nZONE: " + az.name + " IN REGION: " + region.name)
          println("  INSTANCES: " + az.instances)
          println("  PROXIMALS: " + az.proximalTo)
        }
      }*/
    }
    "parse a valid region with no valid zones" in {
      val settings = new ClusterSettings(availabilityZonesWithErrors.withFallback(system.settings.config), system.name)
      import settings._
      val topology = NetworkTopology(TopologyAvailabilityZones)
      topology.regions.size must be(1)
      topology.zones.size must be(0)
      topology.zones.flatMap(_.instances).size must be(0)
      topology.zones.forall(_.instances.size == 0) must be(true)
    }
    "parse the default with no zones (not enabled)" in {
      val settings = new ClusterSettings(defaultAvailabilityZones.withFallback(system.settings.config), system.name)
      import settings._
      val topology = NetworkTopology(TopologyAvailabilityZones)
      topology.regions.size must be(0)
      topology.zones.size must be(0)
      topology.zones.flatMap(_.instances).size must be(0)
      topology.zones.forall(_.instances.size == 0) must be(true)
    }
  }
}

object NetworkTopologyConfigSpec {

  val availabilityZones = ConfigFactory.parseString("""
                                                       |akka.cluster.network-topology.availability-zones {
                                                       |  eu-west-1 {
                                                       |    eu-west-1a {
                                                       |      instances = ["zoo.rack0.node1", "zoo.rack0.node2", "zoo.rack0.node3"]
                                                       |    }
                                                       |    us-west-1b {
                                                       |      instances = ["zoo.rack1.node1", "zoo.rack1.node2", "zoo.rack1.node3"]
                                                       |    }
                                                       |    us-west-1c {
                                                       |      instances = ["zoo.rack2.node1", "zoo.rack2.node2", "zoo.rack2.node3"]
                                                       |    }
                                                       |    us-west-1d {
                                                       |      instances = ["zoo.rack3.node1", "zoo.rack3.node2", "zoo.rack3.node3"]
                                                       |    }
                                                       |  }
                                                       |  us-east-1 {
                                                       |    us-east-1a {
                                                       |      instances = ["zoo.rack0.node1", "zoo.rack0.node2", "zoo.rack0.node3"]
                                                       |    }
                                                       |    us-east-1b {
                                                       |      instances = ["zoo.rack1.node1", "zoo.rack1.node2", "zoo.rack1.node3"]
                                                       |    }
                                                       |    us-east-1c {
                                                       |      instances = ["zoo.rack2.node1", "zoo.rack2.node2", "zoo.rack2.node3"]
                                                       |    }
                                                       |    us-east-1d {
                                                       |      instances = ["zoo.rack3.node1", "zoo.rack3.node2", "zoo.rack3.node3"]
                                                       |    }
                                                       |  }
                                                       |}
                                                     """.stripMargin)

  val availabilityZonesWithErrors = ConfigFactory.parseString("""
                                                                 akka.cluster.network-topology.availability-zones {
                                                                 |  us-east-1 {
                                                                 |    us-east-1a {
                                                                 |    }
                                                                 |    us-east-1b {
                                                                 |      instances = []
                                                                 |    }
                                                                 |    us-east-1b {
                                                                 |      instances = [""]
                                                                 |    }
                                                                 |  }
                                                                 |}
                                                               """.stripMargin)

  val defaultAvailabilityZones = ConfigFactory.parseString("""
      akka.cluster.network-topology {
       availability-zones {

       }
      }""")
}

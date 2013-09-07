package akka.cluster

import akka.testkit.AkkaSpec
import com.typesafe.config._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class NetworkTopologyConfigSpec extends AkkaSpec {

  import ClusterNetworkTopology._
  import NetworkTopologyConfigSpec._

  "NetworkTopology" must {
    "parse valid availability dataCenters" in {
      val settings = new ClusterSettings(configured.withFallback(system.settings.config), system.name)
      import settings._
      val topology = Topology(TopologyAvailabilityZones)
      topology.regions.size must be(2)
      topology.regions forall (_.dataCenters.size == 4) must be(true)
      topology.dataCenters.size must be(8)
      topology.dataCenters forall (_.instances.size == 3) must be(true)
    }
    "parse a valid region with no valid dataCenters" in {
      val settings = new ClusterSettings(withErrors.withFallback(system.settings.config), system.name)
      import settings._
      val topology = Topology(TopologyAvailabilityZones)
      topology.regions.size must be(0)
      topology.dataCenters.size must be(0)
      topology.dataCenters.flatMap(_.instances).size must be(0)
      topology.dataCenters forall (_.instances.size == 0) must be(true)
    }
    "parse the default with no dataCenters (not enabled)" in {
      val settings = new ClusterSettings(default.withFallback(system.settings.config), system.name)
      import settings._
      val topology = Topology(TopologyAvailabilityZones)
      topology.regions.size must be(0)
      topology.dataCenters.size must be(0)
      topology.dataCenters.flatMap(_.instances).size must be(0)
      topology.dataCenters forall (_.instances.size == 0) must be(true)
    }
  }
}

object NetworkTopologyConfigSpec {

  val default = ConfigFactory.parseString("""akka.cluster.topology.data-centers { }""")

  val configured = ConfigFactory.parseString("""
                                                |akka.cluster.topology.data-centers {  # DCs by region
                                                |  eu-west-1 {
                                                |    eu-west-1a {
                                                |      zone-id = 0
                                                |      proximal-to = [1,2,3]
                                                |      instances = ["zoo.rack0.node1", "zoo.rack0.node2", "zoo.rack0.node3"]
                                                |    }
                                                |    eu-west-1b {
                                                |      zone-id = 1
                                                |      proximal-to = [0,2,3]
                                                |      instances = ["zoo.rack1.node1", "zoo.rack1.node2", "zoo.rack1.node3"]
                                                |    }
                                                |    eu-west-1c {
                                                |      zone-id = 2
                                                |      proximal-to = [0,1,3]
                                                |      instances = ["zoo.rack2.node1", "zoo.rack2.node2", "zoo.rack2.node3"]
                                                |    }
                                                |    eu-west-1d {
                                                |      zone-id = 3
                                                |      proximal-to = [0,1,2]
                                                |      instances = ["zoo.rack3.node1", "zoo.rack3.node2", "zoo.rack3.node3"]
                                                |    }
                                                |  }
                                                |  us-east-1 {
                                                |    us-east-1a {
                                                |      zone-id = 0
                                                |      proximal-to = [1,2,3]
                                                |      instances = ["zoo.rack0.node1", "zoo.rack0.node2", "zoo.rack0.node3"]
                                                |    }
                                                |    us-east-1b {
                                                |      zone-id = 1
                                                |      proximal-to = [0,2,3]
                                                |      instances = ["zoo.rack1.node1", "zoo.rack1.node2", "zoo.rack1.node3"]
                                                |    }
                                                |    us-east-1c {
                                                |      zone-id = 2
                                                |      proximal-to = [0,1,3]
                                                |      instances = ["zoo.rack2.node1", "zoo.rack2.node2", "zoo.rack2.node3"]
                                                |    }
                                                |    us-east-1d {
                                                |      zone-id = 3
                                                |      proximal-to = [0,1,2]
                                                |      instances = ["zoo.rack3.node1", "zoo.rack3.node2", "zoo.rack3.node3"]
                                                |    }
                                                |  }
                                                |}
                                              """.stripMargin)

  val withErrors = ConfigFactory.parseString("""
                                                |akka.cluster.topology.data-centers {
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
}

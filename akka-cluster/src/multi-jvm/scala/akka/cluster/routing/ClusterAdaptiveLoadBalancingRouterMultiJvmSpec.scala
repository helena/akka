/*
 * Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing

import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig}
import com.typesafe.config.ConfigFactory
import akka.testkit.{LongRunningTest, DefaultTimeout, ImplicitSender}
import akka.actor._
import akka.routing.{RoutedActorRef, CurrentRoutees, RoundRobinRouter}
import akka.cluster.MultiNodeClusterSpec
import akka.cluster.routing.ClusterRoundRobinRoutedActorMultiJvmSpec.{DeployRoutee, RouteeType, SomeActor}
import dispatch.ActorModelSpec.Reply

object ClusterAdaptiveLoadBalancingRouterMultiJvmSpec extends MultiNodeConfig {

  val first = role("first")
  val second = role("second")
 // TODO 3 more

  // TODO make false
  commonConfig(debugConfig(on = false).
    withFallback(ConfigFactory.parseString("""
      #common-router-settings = {
          #router = consistent-hashing
          #nr-of-instances = 3
          #cluster {
          #  enabled = on
          #  max-nr-of-instances-per-node = 2
          #}
      #}
    """)).
    withFallback(MultiNodeClusterSpec.clusterConfig))

}

class ClusterAdaptiveLoadBalancingRouterMultiJvmNode1 extends ClusterAdaptiveLoadBalancingRouterSpec
class ClusterAdaptiveLoadBalancingRouterMultiJvmNode2 extends ClusterAdaptiveLoadBalancingRouterSpec

abstract class ClusterAdaptiveLoadBalancingRouterSpec extends MultiNodeSpec(ClusterAdaptiveLoadBalancingRouterMultiJvmSpec)
with MultiNodeClusterSpec
with ImplicitSender with DefaultTimeout {
  import ClusterAdaptiveLoadBalancingRouterMultiJvmSpec._

  //routeesPath = "/user/lbWorker",
  lazy val router1 = system.actorOf(Props[SomeActor].withRouter(ClusterRouterConfig(ClusterAdaptiveMemoryLoadBalancingRouter(),
    ClusterRouterSettings(totalInstances = 3, maxInstancesPerNode = 1))), "router1")

  lazy val router2 = system.actorOf(Props[SomeActor].withRouter(ClusterRouterConfig(ClusterAdaptiveMemoryLoadBalancingRouter(),
    ClusterRouterSettings(totalInstances = 3, maxInstancesPerNode = 1))), "router2")
  /*def receiveReplies(routeeType: RouteeType, expectedReplies: Int): Map[Address, Int] = {
    val zero = Map.empty[Address, Int] ++ roles.map(address(_) -> 0)
    (receiveWhile(5 seconds, messages = expectedReplies) {
      case Reply(`routeeType`, ref) ⇒ fullAddress(ref)
    }).foldLeft(zero) {
      case (replyMap, address) ⇒ replyMap + (address -> (replyMap(address) + 1))
    }
  }*/

  "A cluster AdaptiveLoadBalancingRouter with a RoundRobin router" must {
    "start cluster with 2 nodes" taggedAs LongRunningTest in {
      awaitClusterUp(first, second)
      enterBarrier("cluster-started")
      awaitCond(clusterView.clusterMetrics.size == 2)
      enterBarrier("cluster-metrics-published")
    }
    "deploy routees to the member nodes in the cluster" taggedAs LongRunningTest in {

      runOn(first) {
        router1.isInstanceOf[RoutedActorRef] must be(true)

        val iterationCount = 10
        for (i ← 0 until iterationCount) {
          router1 ! "hit"
        }

        /*val replies = receiveReplies(DeployRoutee, iterationCount)

        replies(first) must be > (0)
        replies(second) must be > (0)
        replies.values.sum must be(iterationCount)*/
      }

      enterBarrier("after-2")
    }

  }
}

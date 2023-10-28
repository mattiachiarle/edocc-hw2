
import NetGraphAlgebraDefs.{NetGraph, NodeObject}
import com.typesafe.config.ConfigFactory
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.google.common.graph.EndpointPair
import org.slf4j.LoggerFactory
import RandomWalk.randomWalk


import java.util
import scala.collection._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class valuableNode(node: NodeObject, neighbors: VertexRDD[Array[(VertexId,NodeObject)]])
object Main {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()

    val original = config.getString("Graphs.fileName") //One of the configuration parameters is the name of the file
    val perturbed = s"${original}.perturbed"

    val originalGraph = NetGraph.load(original, "/Users/mattia/repositories/edocc-hw2/untitled/")
    val perturbedGraph = NetGraph.load(perturbed, "/Users/mattia/repositories/edocc-hw2/untitled/")

    val conf = new SparkConf().setAppName("RandomWalk").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Steps:

    //Get the interesting nodes
    //Walk on perturbed
    //For each perturbed node compute the match with the interesting original ones
    //Do it N times, where N is a randomly chosen parameter
    //Each time select the future node randomly
    //Dictionary perturbed node - best match with interesting nodes
    //If the comparison value of at least one perturbed node is higher than a threshold, pick the maximum and attack it
    //In the future walks, remove the interesting node attacked and try to avoid the perturbed node attacked. if it's impossible
    // to avoid it, ignore it in the computations.

    (originalGraph, perturbedGraph) match {
      case (Some(originalGraph), Some(perturbedGraph)) =>

        val originalNodesSeq = originalGraph.sm.nodes().toSeq
        val originalEdgeSeq : scala.Seq[EndpointPair[NodeObject]] = originalGraph.sm.edges().toSeq

        logger.info(s"Number of vertices - original: ${originalGraph.sm.nodes().size()}")

        val originalNodes : RDD[(VertexId,NodeObject)] = sc.parallelize(originalNodesSeq.map(n => (n.id,n)))
        val originalEdges : RDD[Edge[EndpointPair[NodeObject]]] = sc.parallelize(originalEdgeSeq.map(e => Edge(e.nodeU().id,e.nodeV().id)))

        val oGraph = Graph(originalNodes,originalEdges)
        val isEmpty = oGraph.vertices.isEmpty()
        if(isEmpty)
          logger.error("No vertices")

//        val valuableNodes = originalNodes.filter(o => o._2.valuableData).map(o => o._1).collect()

//        val valuable = oGraph.collectNeighbors(EdgeDirection.Either)
//          .filter(v => (v!=null).&&(valuableNodes.contains(v._1)))
//          .map(v => (oGraph.vertices.filter(v2 => v._1 == v2._1).collect()(0)._2,v._2.map(v2 => {
//            if(v2==null){
//              v2
//            }
//            else{
//              v2._2
//            }
//          })))
//          .collect()

        val valuable = mutable.ArrayBuffer[(NodeObject,Array[NodeObject])]()

        val valuableNodes = originalGraph.sm.nodes().filter(n => n.valuableData)
        val valuableIds = valuableNodes.map(n => n.id).toArray

        valuableNodes.foreach(n => {
          val neighbors = mutable.ArrayBuffer[NodeObject]()
          originalGraph.sm.predecessors(n).foreach(p => neighbors += p)
          originalGraph.sm.successors(n).foreach(s => neighbors += s)

          valuable += Tuple2(n,neighbors.toArray)
        })

//        logger.info("Valuable nodes: ")
//        valuable.foreach(v => println(v))

        sc.broadcast(valuable)

        val maxOriginal = originalNodes.map(n => n._1).collect().max

        sc.broadcast(maxOriginal)

        val perturbedNodesSeq = perturbedGraph.sm.nodes().toSeq
        val perturbedEdgeSeq: scala.Seq[EndpointPair[NodeObject]] = perturbedGraph.sm.edges().toSeq

        logger.info(s"Number of vertices - perturbed: ${perturbedGraph.sm.nodes().size()}")

        val perturbedNodes: RDD[(VertexId, NodeObject)] = sc.parallelize(perturbedNodesSeq.map(n => (n.id, n)))
        val perturbedEdges: RDD[Edge[EndpointPair[NodeObject]]] = sc.parallelize(perturbedEdgeSeq.map(e => Edge(e.nodeU().id, e.nodeV().id)))

        val pGraph = Graph(perturbedNodes, perturbedEdges)

        var success = 0
        var fail = 0
        var discover = 0

        for (_ <- 0 until config.getInt("Walks.nWalks")){
          val randomIndex = pGraph.pickRandomVertex()
          val startingNode = perturbedNodes.filter(n => n._1 == randomIndex)
          val attackedNode = randomWalk(valuable,pGraph,startingNode)
          if(attackedNode != null){
            if(attackedNode.id > maxOriginal){
              discover += 1
            }
            else {
              if (valuableIds.contains(attackedNode.id)) {
                success += 1
              }
              else {
                fail += 1
              }
            }
          }
        }

        logger.info(s"$success successes, $discover discover, $fail fail")


    }

    sc.stop()

  }
}
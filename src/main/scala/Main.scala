import NetGraphAlgebraDefs.NodeObject
import com.typesafe.config.ConfigFactory
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import cats.data._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import RandomWalk.randomWalk
import LoadGraph.loadGraph

import java.util
import scala.collection._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection._
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.stream.Collectors
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Main {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("RandomWalk").setMaster("local[*]")
//    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("RandomWalk").getOrCreate()
    val sc = spark.sparkContext

    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()

    val originalEdgesUrl = new URL("https://edocc-homework2.s3.us-east-2.amazonaws.com/graphs/originalEdges")
    val originalEdgesInputStream: InputStream = originalEdgesUrl.openStream()
    val originalEdgesReader = new BufferedReader(new InputStreamReader(originalEdgesInputStream))

    val originalEdges = mutable.ArrayBuffer[Array[Int]]()
    val originalEdgeslines = mutable.ArrayBuffer[String]()

    var line = originalEdgesReader.readLine()

    while(line!=null){
      originalEdgeslines += line
      line = originalEdgesReader.readLine()
    }

    originalEdgeslines.foreach(l => {
      originalEdges += l.split(" ").map(n => n.toInt)
    })
//
//    originalEdges.foreach(e => println(s"${e(0)} ${e(1)}"))
//
    val perturbedEdgesUrl = new URL("https://edocc-homework2.s3.us-east-2.amazonaws.com/graphs/perturbedEdges")
    val perturbedEdgesInputStream: InputStream = perturbedEdgesUrl.openStream()
    val perturbedEdgesReader = new BufferedReader(new InputStreamReader(perturbedEdgesInputStream))

    val perturbedEdges = mutable.ArrayBuffer[Array[Int]]()
    val perturbedEdgeslines = mutable.ArrayBuffer[String]()

    line = perturbedEdgesReader.readLine()

    while (line != null) {
      perturbedEdgeslines += line
      line = perturbedEdgesReader.readLine()
    }

    perturbedEdgeslines.foreach(l => {
      perturbedEdges += l.split(" ").map(n => n.toInt)
    })
//
    val originalNodesUrl = new URL("https://edocc-homework2.s3.us-east-2.amazonaws.com/graphs/originalNodes")
    val originalNodesInputStream: InputStream = originalNodesUrl.openStream()
    val originalNodesReader = new BufferedReader(new InputStreamReader(originalNodesInputStream))

    val originalNodeslines = mutable.ArrayBuffer[String]()

    line = originalNodesReader.readLine()

    logger.info(line)

    while (line != null) {
      originalNodeslines += line
      line = originalNodesReader.readLine()
    }

    val originalNodes = originalNodeslines.map(n => decode[NetGraphAlgebraDefs.NodeObject](n).toOption).collect { case Some(nodeObject) => nodeObject }.toSeq

    val perturbedNodesUrl = new URL("https://edocc-homework2.s3.us-east-2.amazonaws.com/graphs/perturbedNodes")
    val perturbedNodesInputStream: InputStream = perturbedNodesUrl.openStream()
    val perturbedNodesReader = new BufferedReader(new InputStreamReader(perturbedNodesInputStream))

    val perturbedNodeslines = mutable.ArrayBuffer[String]()

    line = perturbedNodesReader.readLine()

    logger.info(line)

    while (line != null) {
      perturbedNodeslines += line
      line = perturbedNodesReader.readLine()
    }

    val perturbedNodes = perturbedNodeslines.map(n => decode[NetGraphAlgebraDefs.NodeObject](n).toOption).collect { case Some(nodeObject) => nodeObject }.toSeq

//    val originalNodesSeq = originalNodes
//    val originalEdgeSeq: scala.Seq[Int] = originalEdges.toSeq
//
//    logger.info(s"Number of vertices - original: ${originalGraph.sm.nodes().size()}")

    val originalNodesRDD: RDD[(VertexId, NodeObject)] = sc.parallelize(originalNodes.map(n => (n.id.toLong, n)))
    val originalEdgesRDD = sc.parallelize(originalEdges.map(e => Edge[VertexId](e(0), e(1))))

    val oGraph = Graph(originalNodesRDD, originalEdgesRDD)
    val isEmptyOriginal = oGraph.vertices.isEmpty()
    if (isEmptyOriginal)
      logger.error("No vertices")

    val perturbedNodesRDD: RDD[(VertexId, NodeObject)] = sc.parallelize(perturbedNodes.map(n => (n.id.toLong, n)))
    val perturbedEdgesRDD = sc.parallelize(perturbedEdges.map(e => Edge[VertexId](e(0), e(1))))

    val pGraph = Graph(perturbedNodesRDD, perturbedEdgesRDD)
    val isEmptyPerturbed = pGraph.vertices.isEmpty()
    if (isEmptyPerturbed)
      logger.error("No vertices")

        logger.info("Test completed")
//
//    originalNodes.foreach(n => logger.info(s"${n.id}"))


//    val originalName = config.getString("Graphs.fileName") //One of the configuration parameters is the name of the file
//    val perturbedName = s"${originalName}.perturbed"
//
//    logger.info(s"Loading the graphs from ${args(0)}$originalName")

//    val originalNodesFile = sc.textFile(s"${args(0)}/originalNodes")
//    val perturbedNodesFile = sc.textFile(s"${args(0)}/perturbedNodes")
//
//    val originalEdgesFile = sc.textFile(s"${args(0)}/originalEdges")
//    val perturbedEdgesFile = sc.textFile(s"${args(0)}/perturbedEdges")
//
//    originalEdgesFile.collect().foreach(e => {
//      logger.info(s"$e")
//    })
//
//    val rdd = sc.textFile(args(0))
//    rdd.collect().foreach(e => {
//      logger.info(s"$e")
//    })




//    val originalGraph = load(originalName,args(0))
//    val perturbedGraph = load(perturbedName,args(0))

//    val oGraph = loadGraph(s"${args(0)}$originalName")(sc)
//    val pGraph = loadGraph(s"${args(0)}$perturbedName")(sc)

//    Steps:
//
//    Get the interesting nodes
//    Walk on perturbed
//    For each perturbed node compute the match with the interesting original ones
//    Do it N times, where N is a randomly chosen parameter
//    Each time select the future node randomly
//    Dictionary perturbed node - best match with interesting nodes
//    If the comparison value of at least one perturbed node is higher than a threshold, pick the maximum and attack it
//    In the future walks, remove the interesting node attacked and try to avoid the perturbed node attacked. if it's impossible
//     to avoid it, ignore it in the computations.

//    (oGraph, pGraph) match {
//      case (Some(oGraph), Some(pGraph)) =>
//
////        val originalNodesSeq = originalGraph.sm.nodes().toSeq
////        val originalEdgeSeq : scala.Seq[EndpointPair[NodeObject]] = originalGraph.sm.edges().toSeq
////
////        logger.debug(s"Number of vertices - original: ${originalGraph.sm.nodes().size()}")
////
////        val originalNodes : RDD[(VertexId,NodeObject)] = sc.parallelize(originalNodesSeq.map(n => (n.id,n)))
////        val originalEdges : RDD[Edge[EndpointPair[NodeObject]]] = sc.parallelize(originalEdgeSeq.map(e => Edge(e.nodeU().id,e.nodeV().id)))
////
////        val oGraph = Graph(originalNodes,originalEdges)
//
        logger.info("Created the RDD of the original graph")
//
//        val isEmpty = oGraph.vertices.isEmpty()
//        if(isEmpty) {
//          logger.error("No vertices")
//        }
//
        val valuableNodes = oGraph.vertices.filter(n => n._2.valuableData).map(n => n._2).collect()
        val valuableIds = valuableNodes.map(n => n.id)

        val valuable : Array[(NodeObject,Array[NodeObject])] = oGraph.collectNeighbors(EdgeDirection.Out).filter(n => valuableIds.contains(n._1.toInt)).collect().map(n => (oGraph.vertices.lookup(n._1).head,n._2.map(n3 => n3._2)))
//          originalGraph.sm.predecessors(n).foreach(p => neighbors += p)
//          originalGraph.sm.successors(n).foreach(s => neighbors += s)

        valuable.foreach(n => {
          if(n._1==null){
            logger.error("ERROR - n1 null")
            return
          }

          if (n._2 == null) {
            logger.error("ERROR - n2 null")
            return
          }
        })

//          valuable += Tuple2(n,neighbors.toArray)

        sc.broadcast(valuable)

        logger.debug(s"Created the array of valuable nodes, with size ${valuable.length}")

        val maxOriginal = oGraph.vertices.map(n => n._1).max()

        logger.debug(s"Maximum id ${maxOriginal}")

        sc.broadcast(maxOriginal)

//        val perturbedNodesSeq = perturbedGraph.sm.nodes().toSeq
//        val perturbedEdgeSeq: scala.Seq[EndpointPair[NodeObject]] = perturbedGraph.sm.edges().toSeq
//
//        logger.debug(s"Number of vertices - perturbed: ${perturbedGraph.sm.nodes().size()}")
//
//        val perturbedNodes: RDD[(VertexId, NodeObject)] = sc.parallelize(perturbedNodesSeq.map(n => (n.id, n)))
//        val perturbedEdges: RDD[Edge[EndpointPair[NodeObject]]] = sc.parallelize(perturbedEdgeSeq.map(e => Edge(e.nodeU().id, e.nodeV().id)))
//
//        val pGraph = Graph(perturbedNodes, perturbedEdges)
//
//        logger.info("Created the RDD of the perturbed graph")

        var success = 0
        var fail = 0
        var discover = 0

        for (_ <- 0 until config.getInt("Walks.nWalks")){
          val randomIndex = pGraph.pickRandomVertex()
          val startingNode = pGraph.vertices.filter(n => n._1==randomIndex)
          logger.info("Starting an attack")
          val attackedNode = randomWalk(valuable,pGraph,startingNode)
          logger.info("Attack ended")
          if(attackedNode != null){
            if(attackedNode.id > maxOriginal){
              discover += 1
              logger.error(s"Node ${attackedNode.id} was an honeypot. You were discovered")
            }
            else {
              if (valuableIds.contains(attackedNode.id)) {
                success += 1
                logger.info(s"The attack to node ${attackedNode.id} was successful!")
              }
              else {
                fail += 1
                logger.warn(s"Node ${attackedNode.id} didn't contain valuable data, but you weren't discovered")
              }
            }
          }
        }

        val attempts = config.getInt("Walks.nWalks")

        logger.info(s"Success rate = ${success.toDouble/attempts.toDouble}")
        logger.info(s"Discovery rate = ${discover.toDouble/attempts.toDouble}")
        logger.info(s"Failure rate = ${fail.toDouble/attempts.toDouble}")
        logger.info(s"No attack rate = ${(attempts-(success+discover+fail)).toDouble/attempts.toDouble}")
//    }

    sc.stop()

  }
}
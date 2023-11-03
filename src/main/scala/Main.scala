import NetGraphAlgebraDefs.NodeObject
import com.typesafe.config.ConfigFactory
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import RandomWalk.RandomWalk.randomWalk
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import scala.collection.mutable.ArrayBuffer

object Main {
  val logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {

    /*
    Uncomment the first two lines and comment the following two lines if you want to run it locally
     */

//    val conf = new SparkConf().setAppName("RandomWalk").setMaster("local[*]")
//    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("RandomWalk").getOrCreate()
    val sc = spark.sparkContext

    val config = ConfigFactory.load()

    /*
    Load edges and nodes from files
     */

    logger.info("Read operation started")

    val originalEdges = readEdges(args(0),"originalEdges")
    val perturbedEdges = readEdges(args(0),"perturbedEdges")

    val originalNodes = readNodes(args(0),"originalNodes")
    val perturbedNodes = readNodes(args(0),"perturbedNodes")

    logger.info("Read operation completed")

    /*
    Create RDDs
     */

    logger.info("Graph creation started")

    val originalNodesRDD: RDD[(VertexId, NodeObject)] = sc.parallelize(originalNodes.map(n => (n.id.toLong, n)))
    val originalEdgesRDD = sc.parallelize(originalEdges.map(e => Edge[VertexId](e(0), e(1))))

    val oGraph = Graph(originalNodesRDD, originalEdgesRDD)
    val isEmptyOriginal = oGraph.vertices.isEmpty()
    if (isEmptyOriginal)
      logger.error("No vertices in original graph")

    val perturbedNodesRDD: RDD[(VertexId, NodeObject)] = sc.parallelize(perturbedNodes.map(n => (n.id.toLong, n)))
    val perturbedEdgesRDD = sc.parallelize(perturbedEdges.map(e => Edge[VertexId](e(0), e(1))))

    val pGraph = Graph(perturbedNodesRDD, perturbedEdgesRDD)
    val isEmptyPerturbed = pGraph.vertices.isEmpty()
    if (isEmptyPerturbed)
      logger.error("No vertices in perturbed graph")

    logger.info("Graphs created")

//    Steps:
//
//    Get the interesting nodes
//    Walk on perturbed
//    For each perturbed node compute the match with the interesting original ones
//    Do it N times, where N is a randomly chosen parameter
//    Each time select the future node randomly
//    Store the best match with interesting nodes
//    Attack the best match if match value<threshold

    val valuableNodes = oGraph.vertices.filter(n => n._2.valuableData).map(n => n._2).collect() //We get the Array of valuable nodes
    val valuableIds = valuableNodes.map(n => n.id) // Useful for the filter operation below

    val valuable : Array[(NodeObject,Array[NodeObject])] = oGraph.collectNeighbors(EdgeDirection.Out)
      .filter(n => valuableIds.contains(n._1.toInt))
      .collect()
      .map(n => (oGraph.vertices.lookup(n._1).head,n._2.map(n3 => n3._2))) //We get the neighbors of all the valuable nodes. They'll be needed for the comparison

    valuable.foreach(n => { //Check introduced for safety
      if(n._1==null){
        logger.error("ERROR - n1 null")
        return
      }

      if (n._2 == null) {
        logger.error("ERROR - n2 null")
        return
      }
    })

    logger.debug("Valuable nodes:")
    valuableNodes.foreach(n => logger.debug(s"$n.id"))

    sc.broadcast(valuable)

    logger.debug(s"Created the array of valuable nodes, with size ${valuable.length}")

    val maxOriginal = oGraph.vertices.map(n => n._1).max() // Useful to understand which nodes are honeypots (i.e. nodes added in the perturbed graph)

    logger.debug(s"Maximum id ${maxOriginal}")

    sc.broadcast(maxOriginal)

    var success = 0 // Number of successful attacks
    var fail = 0 // Number of attacks to nodes without valuable data
    var discover = 0 // Number of attacks to honeypots

    for (_ <- 0 until config.getInt("Walks.nWalks")){ //We perform multiple walks
      val randomIndex = pGraph.pickRandomVertex() // We pick a random starting node
      val startingNode = pGraph.vertices.filter(n => n._1==randomIndex) // We retrieve the original node
      logger.info("Starting an attack")
      val attackedNode = randomWalk(valuable,pGraph,startingNode)
      logger.info("Attack ended")
      if(attackedNode != null){ // Again put for safety
        if(attackedNode.id > maxOriginal){ // Honeypot
          discover += 1
          logger.error(s"Node ${attackedNode.id} was an honeypot. You were discovered")
        }
        else {
          if (valuableIds.contains(attackedNode.id)) { // Valuable node
            success += 1
            logger.info(s"The attack to node ${attackedNode.id} was successful!")
          }
          else { // Non valuable node
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
    logger.info(s"No attack rate = ${(attempts-(success+discover+fail)).toDouble/attempts.toDouble}") // Walks in which we didn't find any node that was worth to attack

    sc.stop()
  }

  def readNodes(path: String, filename: String): Seq[NodeObject] = {
    logger.info(s"Reading nodes from $path$filename")

    val nodesUrl = new URL(s"$path$filename")
    val nodesInputStream: InputStream = nodesUrl.openStream()
    val nodesReader = new BufferedReader(new InputStreamReader(nodesInputStream))

    val nodesLines = mutable.ArrayBuffer[String]()

    var line = nodesReader.readLine()

    while (line != null) {
      logger.debug(s"Read $line")
      nodesLines += line
      line = nodesReader.readLine()
    }

    val nodes = nodesLines.map(n => decode[NetGraphAlgebraDefs.NodeObject](n).toOption).collect { case Some(nodeObject) => nodeObject }.toSeq

    nodes.foreach(n => logger.info(s"$n"))

    return nodes

  }

  def readEdges(path: String, filename: String): ArrayBuffer[Array[Int]] = {
    logger.info(s"Reading edges from $path$filename")

    val edgesUrl = new URL(s"$path$filename")
    val edgesInputStream: InputStream = edgesUrl.openStream()
    val edgesReader = new BufferedReader(new InputStreamReader(edgesInputStream))

    val edges = mutable.ArrayBuffer[Array[Int]]()
    val edgeslines = mutable.ArrayBuffer[String]()

    var line = edgesReader.readLine()

    while (line != null) {
      logger.debug(s"Read $line")
      edgeslines += line
      line = edgesReader.readLine()
    }

    edgeslines.foreach(l => {
      edges += l.split(" ").map(n => n.toInt)
    })

    edges.foreach(e => logger.debug(s"${e(0)} ${e(1)}"))

    return edges
  }
}
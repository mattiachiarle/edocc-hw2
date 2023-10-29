import Main.getClass
import NetGraphAlgebraDefs.{Action, NodeObject}
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object RandomWalk {
  def randomWalk(valuable : Array[(NodeObject,Array[NodeObject])], graph : Graph[NodeObject, Action], startingNode: RDD[(VertexId,NodeObject)]): NodeObject = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()
    val threshold = config.getDouble("Comparison.threshold")
    var bestComparison : Double = 10
    var bestNode : NodeObject = null
    var currentNode = startingNode.collect()(0)

    val visitedNodes = mutable.ArrayBuffer[Long]()

    var prevLength = -1

    while(currentNode!=null){
      for (_ <- 0 until config.getInt("Walks.nSteps") if currentNode != null) {
        if(!visitedNodes.contains(currentNode._1)) {
          visitedNodes += currentNode._1
        }
        logger.info(s"Step in walk. Visited node ${currentNode._1}")
        valuable.foreach(v => {
          val currentNeighbors = graph.collectNeighbors(EdgeDirection.Out).filter(n => n._1 == currentNode._1).map(n => n._2.map(n2 => n2._2)).collect()(0)
          if(currentNode._2 == null){
            logger.error("Current node null")
          }
          if (currentNeighbors == null) {
            logger.error("Current neighbors null")
          }
          if (v._1 == null) {
            logger.error("v1 null")
          }
          if (v._2 == null) {
            logger.error("v2 null")
          }
          val similarity = ComputeSimilarity(currentNode._2, currentNeighbors, v._1, v._2)
          if (similarity < bestComparison) {
            logger.info(s"New best similarity found: $similarity, with node ${v._1}")
            bestComparison = similarity
            bestNode = currentNode._2
            if(bestComparison<threshold){
              logger.info(s"Attacking ${currentNode._1}")
              return bestNode
            }
          }
        })
        currentNode = nextStep(graph,currentNode,visitedNodes)
      }

      if (bestComparison < threshold) {
        return bestNode
      }
      else {
        currentNode = startingNode.collect()(0)
        if(prevLength==visitedNodes.length){
          logger.warn("We visited all the available paths and no interesting nodes were found. We won't attack anyone.")
          currentNode=null
        }
        else{
          prevLength=visitedNodes.length
        }
      }
    }

    null
  }

  def nextStep(graph: Graph[NodeObject, Action], current: (VertexId, NodeObject), visited: ArrayBuffer[Long]): (VertexId, NodeObject) = {
    val logger = LoggerFactory.getLogger(getClass)
    val rand = new scala.util.Random

    val nextNodes = graph.collectNeighbors(EdgeDirection.Out).lookup(current._1).head
    val nextNewNodes = nextNodes.filter(n => !visited.contains(n._1))
    if (nextNewNodes.length == 0) {
      if (nextNodes.length == 0) {
        logger.info("No successors! Ending iteration before numSteps")
        null
      }
      else {
        logger.info("We visited all the nodes at this step! We'll try a random node")
        nextNodes(rand.nextInt(nextNodes.length))
      }
    }
    else {
      logger.info("Visiting a new node!")
      nextNewNodes(rand.nextInt(nextNewNodes.length))
    }
  }

  /**
   * Function used to compute the similarity between two nodes. The lower the similarity the closer the nodes are.
   * My implementation considers both a comparison between the target nodes and a comparison of their neighbors to further improve the performances
   *
   * @param node1  target node
   * @param nodes1 neighbors of the target node
   * @param node2  node with which we are performing the comparison
   * @param nodes2 neighbors of node 2
   * @return similarity score
   */
  def ComputeSimilarity(node1: NodeObject, nodes1: Array[NodeObject], node2: NodeObject, nodes2: Array[NodeObject]): Double = {

    var result = ComputeValue(node1, node2)

    if (result == 0) { //If the nodes are exactly the same, we don't need to check the neighbors and we can directly return
      return result
    }

//    if(nodes1 == null){
//      return result
//    }
//
//    if (nodes2 == null) {
//      return result
//    }
    var skipped = 0

    nodes1.foreach(n1 => { //For each neighbor of the first node, we compute the lowest similarity with the neighbors of node 2
      var tmp = Double.MaxValue
      nodes2.foreach(n2 => {
        val res = ComputeValue(n1, n2)
        if (res < tmp) {
          tmp = res
        }
      })
      if(tmp == Double.MaxValue){
        skipped += 1
      }
      else {
        result = result + tmp //We add the lowest similarity to the current result
      }
    })

    result / (nodes1.length-skipped).toDouble //We normalize the result

  }

  /**
   * Function to simply compute the similarity score between two nodes.
   *
   * @param node1 first node
   * @param node2 second node
   * @return similarity score
   */
  private def ComputeValue(node1: NodeObject, node2: NodeObject): Double = {

    //Properties considered: maxBranchingFactor, maxDepth, maxProperties, propValueRange, storedValue

    val config = ConfigFactory.load()

    if(node1==null){
      return Double.MaxValue
    }
    if (node2 == null) {
      return Double.MaxValue
    }

    val cBranching = config.getDouble("Comparison.cBranching")
    val cDepth = config.getDouble("Comparison.cDepth")
    val cProperties = config.getDouble("Comparison.cProperties")
    val cValueRange = config.getDouble("Comparison.cValueRange")
    val cStoredValue = config.getDouble("Comparison.cStoredValue")

    cBranching * (node1.maxBranchingFactor - node2.maxBranchingFactor).abs / SafeDivision(node1.maxBranchingFactor, node2.maxBranchingFactor) +
      cDepth * (node1.maxDepth - node2.maxDepth).abs / SafeDivision(node1.maxDepth, node2.maxDepth) +
      cProperties * (node1.maxProperties - node2.maxProperties).abs / SafeDivision(node1.maxProperties, node2.maxProperties) +
      cValueRange * (node1.propValueRange - node2.propValueRange).abs / SafeDivision(node1.propValueRange, node2.propValueRange) +
      cStoredValue * (node1.storedValue - node2.storedValue).abs / SafeDivision(node1.storedValue, node2.storedValue)
  }

  /**
   * Function used to avoid divisions by 0
   *
   * @param n1 first number
   * @param n2 second number
   * @return the maximum between n1 and n2 or 1 if they're both 0
   */
  private def SafeDivision(n1: Double, n2: Double): Double = {
    if ((n1 != 0).||(n2 != 0)) {
      n1 max n2
    }
    else {
      1.0
    }
  }
}

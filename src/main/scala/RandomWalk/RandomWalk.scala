package RandomWalk

import NetGraphAlgebraDefs.NodeObject
import com.typesafe.config.ConfigFactory
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object RandomWalk {
  /**
   * Function that performs random walks on the graph
   * @param valuable array of target nodes with their neighbors
   * @param graph the perturbed graph on which we perform the walks
   * @param startingNode the starting node of the walk
   * @return the node that we want to attack (null if we didn't find any interesting node)
   */
  def randomWalk(valuable : Array[(NodeObject,Array[NodeObject])], graph : Graph[NodeObject, VertexId], startingNode: RDD[(VertexId,NodeObject)]): NodeObject = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()
    val threshold = config.getDouble("Comparison.threshold")
    var bestComparison : Double = 10 // Unreachable value, so it will be overwritten after the first comparison
    var bestNode : NodeObject = null // Node from the valuable array that provided the best similarity
    var currentNode = startingNode.collect()(0)

    val visitedNodes = mutable.ArrayBuffer[Long]()

    var prevLength = -1 // To check if during a walk we traversed new nodes

    while(currentNode!=null){
      for (_ <- 0 until config.getInt("Walks.nSteps") if currentNode != null) {
        if(!visitedNodes.contains(currentNode._1)) { // If we didn't visit this node yet we add it to the array of visited nodes
          visitedNodes += currentNode._1
        }
        logger.info(s"Step in walk. Visited node ${currentNode._1}")
        valuable.foreach(v => {
          val currentNeighbors = graph.collectNeighbors(EdgeDirection.Out)
            .filter(n => (n!=null).&&(n._1 == currentNode._1))
            .map(n => n._2.map(n2 => n2._2)).collect()(0) // We get the neighbors of the current nodes
          if(currentNode._2 == null){ //Safety checks
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
            logger.info(s"New best similarity found: $similarity, with node ${currentNode._2}")
            bestComparison = similarity
            bestNode = currentNode._2
            if(bestComparison<threshold){ // If the comparison is < threshold we are confident enough that the current node contains valuable data, so we attack it. It's a greedy algorithm
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
        logger.info("Restarting the walk")
        currentNode = startingNode.collect()(0) // We restart the walk from the starting node
        if(prevLength==visitedNodes.length){ // During the last walk we didn't visit any new node, so probably we visited all the available paths
          logger.warn("We visited all the available paths and no interesting nodes were found. We won't attack anyone.")
          currentNode=null // This will stop the iteration
        }
        else{
          prevLength=visitedNodes.length
        }
      }
    }

    null
  }

  /**
   * This function computes the next step, selecting the next node randomly among the successors
   * @param graph the graph that we are visiting
   * @param current the current node
   * @param visited the array of visited nodes
   * @return the next node to visit in the walk
   */
  def nextStep(graph: Graph[NodeObject, VertexId], current: (VertexId, NodeObject), visited: ArrayBuffer[Long]): (VertexId, NodeObject) = {
    val logger = LoggerFactory.getLogger(getClass)
    val rand = new scala.util.Random

    val nextNodes = graph.collectNeighbors(EdgeDirection.Out).lookup(current._1).head //We get all the available next steps
    val nextNewNodes = nextNodes.filter(n => !visited.contains(n._1)) //We get the array of future nodes that we didn't visit yet
    if (nextNewNodes.length == 0) { //All the available next steps have already been visited, so we pick a random node
      if (nextNodes.length == 0) { // There are no next steps available, so we must stop the walk
        logger.info("No successors! Ending iteration before numSteps")
        null
      }
      else {
        logger.info("We visited all the nodes at this step! We'll try a random node")
        nextNodes(rand.nextInt(nextNodes.length))
      }
    }
    else { //We pick a random node among the unvisited ones
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

    val logger = LoggerFactory.getLogger(getClass)

    var result = ComputeValue(node1, node2)

    if (result == 0) { //If the nodes are exactly the same, we don't need to check the neighbors and we can directly return
      logger.info("Found two identical nodes")
      return result
    }

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
        logger.warn("Skipped a node")
        skipped += 1
      }
      else {
        result = result + tmp //We add the lowest similarity to the current result
      }
    })

    if(skipped==nodes1.length){
      logger.warn("We skipped all the neighbors")
      return result*2 //The multiplication by 2 is needed to have better performances
    }

    result / (nodes1.length-skipped).toDouble //We normalize the result

  }

  /**
   * Function to simply compute the similarity score between two nodes.
   *
   * @param node1 first node
   * @param node2 second node
   * @return similarity score
   */
  def ComputeValue(node1: NodeObject, node2: NodeObject): Double = {

    //Properties considered: maxBranchingFactor, maxDepth, maxProperties, propValueRange, storedValue

    val config = ConfigFactory.load()
    val logger = LoggerFactory.getLogger(getClass)

    if(node1==null){
      logger.warn(s"Node 1 null!")
      return Double.MaxValue
    }
    if (node2 == null) {
      logger.warn(s"Node 2 null!")
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

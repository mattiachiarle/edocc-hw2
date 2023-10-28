import Main.getClass
import NetGraphAlgebraDefs.NodeObject
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import com.google.common.graph.EndpointPair
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.mutable.ArrayBuffer

object RandomWalk {
  def randomWalk(valuable : ArrayBuffer[(NodeObject,Array[NodeObject])], graph : Graph[NodeObject, EndpointPair[NodeObject]], startingNode: RDD[(VertexId,NodeObject)]): NodeObject = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()
    val rand = new scala.util.Random
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
          val currentNeighbors = graph.collectNeighbors(EdgeDirection.Either).filter(n => n._1 == currentNode._1).map(n => n._2.map(n2 => n2._2)).collect()(0)
          val similarity = ComputeSimilarity(currentNode._2, currentNeighbors, v._1, v._2)
          if (similarity < bestComparison) {
            bestComparison = similarity
            bestNode = v._1
          }
        })
        val nextNodes = graph.collectNeighbors(EdgeDirection.Out).lookup(currentNode._1).head
        val nextNewNodes = nextNodes.filter(n => !visitedNodes.contains(n._1))
        if(nextNewNodes.length==0) {
          if (nextNodes.length == 0) {
            logger.info("No successors! Ending iteration before numSteps")
            currentNode = null
          }
          else {
            currentNode = nextNodes(rand.nextInt(nextNodes.length))
          }
        }
        else{
          currentNode = nextNewNodes(rand.nextInt(nextNewNodes.length))
        }
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

    return null
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

    nodes1.foreach(n1 => { //For each neighbor of the first node, we compute the lowest similarity with the neighbors of node 2
      var tmp = Double.MaxValue
      nodes2.foreach(n2 => {
        val res = ComputeValue(n1, n2)
        if (res < tmp) {
          tmp = res
        }
      })
      result = result + tmp //We add the lowest similarity to the current result
    })

    result / (nodes1.length) //We normalize the result

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

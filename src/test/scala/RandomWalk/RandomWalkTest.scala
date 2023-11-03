package RandomWalk
import NetGraphAlgebraDefs.NodeObject
import RandomWalk.{ComputeValue, ComputeSimilarity}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RandomWalkTest extends AnyFlatSpec with Matchers with MockFactory{
  behavior of "Compute value"

  it should "provide value=Double.MaxValue with the first node null" in {
    val node = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0, false)

    val res = ComputeValue(null, node)

    res shouldEqual (Double.MaxValue)
  }

  it should "provide value=Double.MaxValue with the second node null" in {
    val node = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0, false)

    val res = ComputeValue(node, null)

    res shouldEqual (Double.MaxValue)
  }

  it should "provide value!=Double.MaxValue with two nodes not null" in {
    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0, false)
    val node2 = new NodeObject(2, 4, 3, 8, 6, 23, 11, 10, 5.4, true)

    val res = ComputeValue(node1, node2)

    res should not be (Double.MaxValue)
  }

  behavior of "Compute similarity"

  it should "provide the similarity between node1 and node2 if node1/node2 has no neighbors" in{
    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0, false)
    val node2 = new NodeObject(2, 4, 3, 8, 6, 23, 11, 10, 5.4, true)

    val similarity = ComputeSimilarity(node1,Array(node1),node2,Array.empty)
    val value = ComputeValue(node1, node2)

    similarity should be (2*value)
  }

  it should "handle skipped nodes properly" in {

    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0, false)
    val node2 = new NodeObject(2, 4, 3, 8, 6, 23, 11, 10, 5.4, true)

    val similarity = ComputeSimilarity(node1, Array(node1,null), node2, Array(node2))
    val value = ComputeValue(node1, node2)

    val theoreticalResult = value * 2

    similarity should be (theoreticalResult)

  }

}

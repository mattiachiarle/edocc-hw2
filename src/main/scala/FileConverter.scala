import java.util
import java.io.{FileOutputStream, ObjectOutputStream, PrintWriter}
import scala.collection._
import org.apache.commons.io.FileUtils
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}

object FileConverter {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()
    val original = config.getString("Graphs.fileName") //One of the configuration parameters is the name of the file
    val perturbed = s"${original}.perturbed"
    val dir = config.getString("Graphs.outputPath") //Where to save the shards
    val sourceDir = config.getString("Graphs.graphLocation") //Where the graphs are located

//    val originalGraph = NetGraph.load(original, sourceDir)
//    val perturbedGraph = NetGraph.load(perturbed, sourceDir)

    val logger = LoggerFactory.getLogger(getClass)

    val originalNodes: PrintWriter = {
      new PrintWriter(s"${dir}/originalNodes")
    }
    val originalEdges: PrintWriter = {
      new PrintWriter(s"${dir}/originalEdges")
    }
    val perturbedNodes: PrintWriter = {
      new PrintWriter(s"${dir}/perturbedNodes")
    }
    val perturbedEdges: PrintWriter = {
      new PrintWriter(s"${dir}/perturbedEdges")
    }

//    (originalGraph, perturbedGraph) match {
//      case (Some(originalGraph), Some(perturbedGraph)) =>
//
//        logger.info("Starting the conversion operation.")
//
//        originalGraph.sm.nodes().forEach(on => {
//          originalNodes.print(s"${on.asJson.noSpaces}\n")
//        })
//
//        perturbedGraph.sm.nodes().forEach(on => {
//          perturbedNodes.print(s"${on.asJson.noSpaces}\n")
//        })
//
//        originalGraph.sm.edges().forEach(oe => {
//          originalEdges.print(s"${oe.nodeU().id} ${oe.nodeV().id}\n")
//        })
//
//        perturbedGraph.sm.edges().forEach(pe => {
//          perturbedEdges.print(s"${pe.nodeU().id} ${pe.nodeV().id}\n")
//        })
//
//        logger.info("Conversion operation completed")
//
//        originalNodes.close()
//        originalEdges.close()
//        perturbedNodes.close()
//        perturbedEdges.close()
//    }
  }
}


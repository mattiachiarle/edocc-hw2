import NetGraphAlgebraDefs.NetGraphComponent
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import java.io.FileInputStream
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io._
import java.io.ObjectInputStream
import scala.util.{Failure, Success, Try}
import java.io._
import java.net.URL

object LoadGraph {
  private val logger = LoggerFactory.getLogger(getClass)

  def loadGraph(fileName: String) = {
    Try {
      val fis = if (fileName.startsWith("http://") || fileName.startsWith("https://")) {
        val url = new URL(fileName)
        url.openStream()
      } else {
        new FileInputStream(new File(fileName))
      }
      val ois = new ObjectInputStream(fis)
      val ng = ois.readObject.asInstanceOf[List[NetGraphAlgebraDefs.NetGraphComponent]]
      ois.close()
      fis.close()
      ng
    } match {
      case Success(lstOfNetComponents) =>
        logger.info("Almost creating graph")
        Some(null)

      case Failure(e: FileNotFoundException) =>
        logger.error(s"File not found: $fileName", e)
        None

      case Failure(e) =>
        logger.error("An error occurred while loading the graph", e)
        None
    }
  }
}
//import Main.getClass
//import NetGraphAlgebraDefs.{NetGraphAlgebraDefs.Action, NetGraph, NetGraphAlgebraDefs.NetGraphComponent, NetModelAlgebra, NetGraphAlgebraDefs.NodeObject}
//import org.slf4j.{Logger, LoggerFactory}
//
//import java.io.{File, FileInputStream, InputStream, ObjectInputStream}
//import java.net.URL
//import scala.util.{Failure, Success, Try}
//@SerialVersionUID(123L)
//object LoadGraphOld extends Serializable{
//
//  def load(fileName: String, dir: String): Option[NetGraph] = {
////    @SerialVersionUID(123L)
//    val logger = LoggerFactory.getLogger(getClass)
//
//    Try {
//      println("Starting read")
//      val url = new URL(s"$dir$fileName")
//      val is = url.openStream()
//      val ois = new ObjectInputStream(is)
//      val ng = ois.readObject.asInstanceOf[List[NetGraphAlgebraDefs.NetGraphComponent]]
//      ois.close()
//      is.close()
//      ng
//    } match {
//      case Success(ng) =>
//        val nodes = ng.collect { case node: NetGraphAlgebraDefs.NodeObject => node }
//        val edges = ng.collect { case edge: NetGraphAlgebraDefs.Action => edge }
//
//        println(s"Deserialized ${nodes.length} nodes and ${edges.length} edges")
//        NetModelAlgebra(nodes, edges)
//
//      case Failure(ex) =>
//        logger.error(s"Failed to load the NetGraph from $dir$fileName", ex)
//        None
//    }
//  }
//}

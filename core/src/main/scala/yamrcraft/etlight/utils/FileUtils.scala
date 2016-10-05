package yamrcraft.etlight.utils

import java.io.{File, FileInputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

object FileUtils {

  /**
    * read the content of the file specified in the given <tt>path</tt>, path could be:
    * - 'file:///local-path' - read from local file-system
    * - otherwise - read from hadoop file-system
    *
    * @param path file path
    * @return the content of the file
    */
  def readContent(path: String) = {
    val configURI = new URI(path)
    val fs = FileSystem.get(new Configuration())

    configURI.getScheme match {
      case "file" => Source.fromInputStream(new FileInputStream(new File(configURI))).mkString
      case _ => Source.fromInputStream(fs.open(new Path(path))).mkString
    }
  }

}

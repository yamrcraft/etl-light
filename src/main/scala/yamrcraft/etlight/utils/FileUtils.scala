package yamrcraft.etlight.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

object FileUtils {

  val config = new Configuration()

  /**
    * read the content of the file specified in the given <tt>uri</tt>, uri could be of the follwoing schemes:
    * - 'file:///local-path' - read from local file-system
    * - 'hdfs:///path - read from hadoop file-system
    * - 's3a://authority/path - read from s3 storage
    *
    * @param uri file uri
    * @return the content of the file
    */
  def readContent(uri: String) = {
    val fs = getFS(uri)
    Source.fromInputStream(fs.open(new Path(uri))).mkString
  }

  def getFS(uri: String): FileSystem = {
    FileSystem.get(URI.create(uri), config)
  }

}

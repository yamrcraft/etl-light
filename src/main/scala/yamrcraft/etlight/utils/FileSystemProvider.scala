package yamrcraft.etlight.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable

object FileSystemProvider {

  val fsCache = mutable.Map[String, FileSystem]()

  def getFS(uri: String): FileSystem = {
    val u = URI.create(uri)

    u.getScheme match {
      case scheme if scheme.startsWith("s3") =>
        val bucketURI = s"$scheme://${u.getAuthority}"
        fsCache.getOrElseUpdate(bucketURI, FileSystem.get(URI.create(bucketURI), new Configuration()))

      case "hdfs" =>
        fsCache.getOrElseUpdate("hdfs", FileSystem.get(new Configuration()))
    }
  }

}

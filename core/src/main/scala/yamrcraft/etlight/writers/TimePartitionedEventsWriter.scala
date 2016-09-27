package yamrcraft.etlight.writers

import java.io.IOException

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import parquet.avro.AvroParquetWriter
import yamrcraft.etlight.utils.ConfigConversions._
import yamrcraft.etlight.{ErrorType, EtlException}

import scala.collection.mutable
import scala.io.Source

/**
	* Lazy events writer - file writer is created on the first call to write.
	*/
class TimePartitionedEventsWriter(config: Config, jobId: Long, partitionId: Int) extends AvroEventsWriter {

	val logger = LoggerFactory.getLogger(this.getClass)

	// config settings
	val workingFolder: String = config.getString("working-folder")
	val outputFolder: String = config.getString("output-folder")
	val partitionField: String = config.getString("partition.field")
	val partitionPattern: String = config.getString("partition.pattern")
	val folders: Map[String, String] = config.getConfig("type-to-folders-mapping").asMap

	val fs = FileSystem.get(new Configuration())

	// maps relative partition file path to a writer instance
	val writers = mutable.Map[Path, AvroParquetWriter[GenericRecord]]()

	val partitionFormat = DateTimeFormat.forPattern(partitionPattern)

	@throws(classOf[EtlException])
	@throws(classOf[IOException])
	override def write(event: GenericRecord) = {
		val timestamp = extractTimestamp(event)
		val schema = event.getSchema
		val eventType = schema.getName

		logger.info(s"Event Type: $eventType")
		val id = s"job${jobId}_part$partitionId"

		logger.info(s"Schema ${schema.getFullName} fields: ${schema.getFields}")
		val writer = writerFor(eventType, id, timestamp, schema)
		writer.write(event)
	}

	override def commit() = {
		// close all writers
		writers.values.foreach(_.close())

		writers foreach { case (relFilePath, writer) =>
			val workingFile = new Path(workingFolder, relFilePath)
			val outputFile = new Path(outputFolder, relFilePath)
			fs.mkdirs(outputFile.getParent)
			if (fs.exists(outputFile)) {
				fs.rename(outputFile, new Path(outputFile.getParent, s"__${outputFile.getName}.${System.currentTimeMillis()}.old.__"))
			}
			fs.rename(workingFile, outputFile)
		}
	}

	@throws(classOf[EtlException])
	private def extractTimestamp(event: GenericRecord): Long = {
		try {
			event.get(partitionField).asInstanceOf[Long]
		} catch {
			case e: Exception => throw new EtlException(ErrorType.PartitionTimestampError, e)
		}
	}

	@throws(classOf[EtlException])
	private def writerFor(name: String, id: String, timestamp: Long, schema: Schema): AvroParquetWriter[GenericRecord] = {
		val relFolder = relativePartitionFolder(folders.getOrElse(name, name), timestamp)
		val relFile = new Path(relFolder, s"events_${name}_$id.parquet")

		writers.getOrElseUpdate(relFile, createWriter(workingFolder, relFile, schema))
	}

	private def relativePartitionFolder(name: String, timestamp: Long) = {
		new Path(name + Path.SEPARATOR + partitionFormat.print(timestamp))
	}

	protected def createWriter(baseFolder: String, relPath: Path, schema: Schema) = {
		val file = new Path(baseFolder, relPath)
		if (fs.exists(file)) fs.delete(file, true)
		new AvroParquetWriter[GenericRecord](file, schema)
	}

	private def readFile(file: String) = {
		val path = new Path(file)
		val source = Source.fromInputStream(fs.open(path))
		try source.mkString finally source.close()
	}

}

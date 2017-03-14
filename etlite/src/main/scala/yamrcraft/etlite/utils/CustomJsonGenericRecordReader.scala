package yamrcraft.etlite.utils

import tech.allegro.schema.json2avro.converter.JsonGenericRecordReader

class CustomJsonGenericRecordReader extends JsonGenericRecordReader {

  override def onValidType[T](value: Any, `type`: Class[T], path: java.util.Deque[String], silently: Boolean, function: java.util.function.Function[T, AnyRef]): AnyRef = {
    if (`type` == classOf[java.lang.String]) {
      value.toString
    } else {
      super.onValidType(value, `type`, path, silently, function)
    }
  }

}

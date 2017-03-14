package yamrcraft.etlite.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object TimeUtils {

  def stringTimeToLong(time: String, format: String): Long =
    DateTime.parse(time, DateTimeFormat.forPattern(format)).getMillis
}

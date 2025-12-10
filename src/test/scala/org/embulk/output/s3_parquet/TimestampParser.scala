package org.embulk.output.s3_parquet

import org.embulk.spi.time.Timestamp
import java.time.{ZoneId, ZoneOffset}

/**
  * Simplified TimestampParser for testing purposes, based on embulk 0.9's TimestampParser.
  * This is a minimal implementation to support the test cases.
  *
  * ref:
  *   - https://github.com/embulk/embulk/blob/maintain-v0.9/embulk-core/src/main/java/org/embulk/spi/time/TimestampParser.java
  *   - https://github.com/embulk/embulk/blob/maintain-v0.9/embulk-core/src/main/java/org/embulk/spi/time/TimestampParserLegacy.java
  */
object TimestampParser {

  def of(formatString: String, defaultZoneIdString: String): TimestampParser = {
    val zoneId = if (defaultZoneIdString == "UTC") {
      ZoneOffset.UTC
    }
    else {
      ZoneId.of(defaultZoneIdString)
    }
    new TimestampParser(formatString, zoneId)
  }
}

class TimestampParser(formatString: String, defaultZoneId: ZoneId) {

  private val defaultYear = 1970
  private val defaultMonthOfYear = 1
  private val defaultDayOfMonth = 1

  /**
    * Parse timestamp string according to the format.
    * This is a simplified implementation that handles the specific format used in tests:
    * "%Y-%m-%d %H:%M:%S.%N %z"
    */
  def parse(text: String): Timestamp = {
    if (text == null || text.isEmpty) {
      throw new RuntimeException("text is null or empty string.")
    }

    // Parse the format: "2017-10-22 19:53:31.000000 +0900"
    // Pattern: YYYY-MM-DD HH:MM:SS.NNNNNN +ZZZZ
    val pattern =
      """(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\.(\d{6})\s+([+-]\d{4})""".r

    text match {
      case pattern(year, month, day, hour, minute, second, nanos, zone) =>
        val y = year.toInt
        val mo = month.toInt
        val d = day.toInt
        val h = hour.toInt
        val mi = minute.toInt
        val s = second.toInt
        val ns = (nanos.toInt * 1000) // Convert microseconds to nanoseconds

        // Parse timezone offset
        val zoneOffsetHours = zone.substring(0, 3).toInt
        val zoneOffsetMinutes = zone.substring(3).toInt
        val totalOffsetSeconds =
          (zoneOffsetHours * 3600) + (if (zoneOffsetHours < 0)
                                        -zoneOffsetMinutes * 60
                                      else zoneOffsetMinutes * 60)
        val zoneOffset = ZoneOffset.ofTotalSeconds(totalOffsetSeconds)

        // Create instant
        val localDateTime = java.time.LocalDateTime.of(y, mo, d, h, mi, s, ns)
        val zonedDateTime =
          java.time.ZonedDateTime.of(localDateTime, zoneOffset)
        val instant = zonedDateTime.toInstant

        Timestamp.ofInstant(instant)

      case _ =>
        throw new RuntimeException(s"Cannot parse '$text' by '$formatString'")
    }
  }
}

package com.ambiata.notion.distcopy

import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.saws.cw._
import com.ambiata.notion.distcopy.DistCopyStats._
import org.joda.time.DateTime

case class DistCopyStats(hadoopJobId: String, counts: Map[String, Long], timestamp: DateTime) {

  /** @return statistics for CloudWatch metrics */
  def statisticsMetrics: StatisticsMetrics =
    StatisticsMetrics(toStatistics, MetricDimensions.extendLongestPrefix(dimensions), timestamp)

  /** @return specific metric dimensions for these statistics */
  def dimensions: List[MetricDimension] =
    List(MetricDimension("job-id", hadoopJobId))

  /** @return CloudWatch statistics */
  def toStatistics: Statistics = Statistics {
    (memoryStats(UPLOADED_BYTES)          ++
     memoryStats(DOWNLOADED_BYTES)        ++
     countStat  (UPLOADED_FILES).toList   ++
     countStat  (DOWNLOADED_FILES).toList ++
     countStat  (RETRIED_FILES).toList).toMap
  }

  /** @return several statistics for different memory units */
  def memoryStats(name: String): List[(String, StatisticsData)] =
    counts.get(name).map(b => List(
        (name,                               StatisticsData(b.toDouble,                         Bytes))
      , (name.replace("bytes", "megabytes"), StatisticsData(b.bytes.toMegabytes.value.toDouble, Megabytes))
      , (name.replace("bytes", "gigabytes"), StatisticsData(b.bytes.toGigabytes.value.toDouble, Gigabytes)))).toList.flatten

  /** @return count statistics */
  def countStat(name: String): Option[(String, StatisticsData)] =
    counts.get(name).map(n => (name, StatisticsData(n.toDouble, Count)))

}

object DistCopyStats {

  val UPLOADED_BYTES   = "uploaded.bytes"
  val DOWNLOADED_BYTES = "downloaded.bytes"
  val UPLOADED_FILES   = "uploaded.files"
  val DOWNLOADED_FILES = "downloaded.files"
  val RETRIED_FILES    = "retried.files"

}


package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.cloudwatch.model._
import com.ambiata.mundane.control.RIO
import com.ambiata.saws.core.CloudWatchAction
import CloudWatchAction._
import scala.collection.JavaConverters._
import com.ambiata.mundane.io._, MemoryConversions._
import scalaz._, Scalaz._

case class DistCopyStats(name: String, counts: Map[String, Long])

object DistCopyStats {

  val UPLOADED_BYTES   = "uploaded.bytes"
  val DOWNLOADED_BYTES = "downloaded.bytes"
  val UPLOADED_FILES   = "uploaded.files"
  val DOWNLOADED_FILES = "downloaded.files"
  val RETRIED_FILES    = "retried.files"

  def publish(stats: DistCopyStats, namespace: Namespace, customDimensions: List[MetricDimension]): CloudWatchAction[Unit] =
    for {
      dimensions <- fromRIO(RIO.fromDisjunctionString(makeDimensions(stats, customDimensions)))
      c          <- client
      put        =  makePutMetricDataRequest(stats, namespace, dimensions)
      _          <- safe(c.putMetricData(put))
    } yield ()

  def makePutMetricDataRequest(stats: DistCopyStats, namespace: Namespace, dimensions: List[Dimension]): PutMetricDataRequest = {
    val put: PutMetricDataRequest = new PutMetricDataRequest()
    put.withNamespace(namespace.n)

    val metrics: List[MetricDatum] = List(
        stats.counts.get(UPLOADED_BYTES  ).map(v => memoryMetrics(UPLOADED_BYTES,  v.bytes)).getOrElse(Nil)
      , stats.counts.get(DOWNLOADED_BYTES).map(v => memoryMetrics(DOWNLOADED_BYTES, v.bytes)).getOrElse(Nil)
      , stats.counts.get(UPLOADED_FILES  ).map(v => countMetric (UPLOADED_FILES,  v)).toList
      , stats.counts.get(DOWNLOADED_FILES).map(v => countMetric (DOWNLOADED_FILES, v)).toList
      , stats.counts.get(RETRIED_FILES   ).map(v => countMetric (RETRIED_FILES, v)).toList
    ).flatten.map(setDimensions(dimensions))

    put.withMetricData(metrics.asJavaCollection)
  }

  /**
   * Make dimensions for distcopy stats: 10 dimensions per metric max are allowed by CloudWatch
   */
  def makeDimensions(stats: DistCopyStats, customDimensions: List[MetricDimension]): String \/ List[Dimension] = {
    val hadoopJobId: Dimension = new Dimension
    hadoopJobId.setName("hadoop-job-id")
    hadoopJobId.setValue(stats.name)

    val dimensions = hadoopJobId :: customDimensions.map(_.toDimension)
    if (dimensions.size > 10) s"""Too many dimensions for a metric. Should be less or equal to 10. Got: ${dimensions.mkString("\n")}""".left
    else dimensions.right
  }

  /** set the dimensions on the metric object */
  def setDimensions(dimensions: List[Dimension]): MetricDatum => MetricDatum = (metric: MetricDatum) => {
    metric.setDimensions(dimensions.asJavaCollection)
    metric
  }

  def memoryMetrics(name: String, value: BytesQuantity): List[MetricDatum] = {
    List(
        value             -> StandardUnit.Bytes
      , value.toMegabytes -> StandardUnit.Megabytes
      , value.toGigabytes -> StandardUnit.Gigabytes
      , value.toTerabytes -> StandardUnit.Terabytes
    ).map { case (v, u) => createMetricDatum(name, v.value.toDouble, u) }
  }

  def countMetric(name: String, value: Long): MetricDatum =
    createMetricDatum(name, value.toDouble, StandardUnit.Count)

  def createMetricDatum(name: String, value: Double, unit: StandardUnit) = {
    val metric = new MetricDatum
    metric.setMetricName(name)
    metric.setValue(value)
    metric.setUnit(unit)
    metric
  }

  def empty: DistCopyStats =
    DistCopyStats("empty", Map.empty)

}

case class Namespace(n: String) extends AnyVal

case class MetricDimension(name: String, value: String) extends {
  def toDimension: Dimension = {
    val d: Dimension = new Dimension
    d.setName(name)
    d.setValue(value)
    d
  }
}


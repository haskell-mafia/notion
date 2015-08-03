package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.ambiata.com.amazonaws.services.cloudwatch.model._
import com.ambiata.saws.core._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Mapper, Counter}

import scala.collection.JavaConverters._

import scalaz._, Scalaz._

case class DistCopyStats(name: String, counts: Map[String, Long])

object DistCopyStats {

  def publish(stats: DistCopyStats): CloudWatchAction[Unit] = {
    val totalBytesUploaded = stats.counts.get("total.bytes.uploaded")
    val totalMegabytesUploaded = stats.counts.get("total.megabytes.uploaded")
    val totalGigabytesUploaded = stats.counts.get("total.gigabytes.uploaded")
    val totalFilesUploaded = stats.counts.get("total.files.uploaded")
    val totalBytesDownloaded = stats.counts.get("total.bytes.downloaded")
    val totalMegabytesDownloaded = stats.counts.get("total.megabytes.downloaded")
    val totalGigabytesDownloaded = stats.counts.get("total.gigabytes.downloaded")
    val totalFilesDownloaded = stats.counts.get("total.files.downloaded")
    val retryCounter = stats.counts.get("total.files.retried")

    val put: PutMetricDataRequest = new PutMetricDataRequest()
    put.withNamespace("Ambiata/View")

    val dim: Dimension = new Dimension()
    dim.setName("Hadoop_Job_Id")
    dim.setValue(stats.name)

    val md: List[MetricDatum] = List(
        totalBytesUploaded.map(metric("UploadByteCount", _, dim))
      , totalBytesUploaded.map(metric("UploadByteCount", _, totals("bytes.uploaded")))
      , totalMegabytesUploaded.map(metric("UploadMegabyteCount", _, totals("megabytes.uploaded")))
      , totalGigabytesUploaded.map(metric("UploadGigabyteCount", _, totals("gigabytes.uploaded")))
      , totalBytesDownloaded.map(metric("DownloadByteCount", _, dim))
      , totalBytesDownloaded.map(metric("DownloadByteCount", _, totals("bytes.downloaded")))
      , totalMegabytesDownloaded.map(metric("DownloadMegabyteCount", _, totals("megabytes.downloaded")))
      , totalGigabytesDownloaded.map(metric("DownloadGigabyteCount", _, totals("gigabytes.downloaded")))
      , retryCounter.map(metric("TotalRetries", _, totals("retry")))
      , totalFilesUploaded.map(metric("TotalFilesUploaded", _, totals("files.uploaded")))
      , totalFilesDownloaded.map(metric("TotalFilesDownloaded", _, totals("files.downloaded")))
    ).flatten
    md.foreach(put.withMetricData(_))
    CloudWatchAction(_.putMetricData(put))
  }

  def totals(s: String): Dimension = {
    val dim: Dimension = new Dimension()
    dim.setName("Totals")
    dim.setValue(s)
    dim
  }

  def metric(name: String, value: Long, dim: Dimension): MetricDatum = {
    val metric: MetricDatum = new MetricDatum()
    metric.setMetricName(name)
    metric.setUnit(StandardUnit.Count)
    metric.setValue(value)
    metric.setDimensions(dim.pure[List].asJava)
    metric
  }

  def empty: DistCopyStats = {
    DistCopyStats("empty", Map.empty)
  }
}

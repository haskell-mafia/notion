package com.ambiata.notion.distcopy

import com.ambiata.saws.cw._
import com.ambiata.saws.cw.Arbitraries._
import org.specs2._
import DistCopyStats._

class DistCopyStatsSpecs extends Specification with ScalaCheck { def is = s2"""

  Statistics can be built from counts
    memory stats $memoryStats
    file stats   $fileStats

"""

  def memoryStats = prop { (jobId: String, bytes: Long, ts: Timestamp) =>
    val stats = DistCopyStats(jobId, Map(UPLOADED_BYTES -> bytes), ts.value)

    stats.memoryStats(UPLOADED_BYTES) === List(
      ("uploaded.bytes",     StatisticsData(bytes.toDouble, Bytes))
    , ("uploaded.megabytes", StatisticsData((bytes / 1024 / 1024).toDouble, Megabytes))
    , ("uploaded.gigabytes", StatisticsData((bytes / 1024 / 1024 / 1024).toDouble, Gigabytes))
    )
  }

  def fileStats = prop { (jobId: String, files: Long, ts: Timestamp) =>
    val stats = DistCopyStats(jobId, Map(UPLOADED_FILES -> files), ts.value)

    stats.countStat(UPLOADED_FILES) === Some(
        ("uploaded.files",     StatisticsData(files.toDouble, Count))
    )
  }


}

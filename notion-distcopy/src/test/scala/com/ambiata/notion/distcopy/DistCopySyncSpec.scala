package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.distcopy.Arbitraries._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.Arbitraries._
import org.specs2._
import org.specs2.matcher.Parameters

class DistCopySyncSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Syncing files between S3 and HDFS
=================================

  Upload and Download file                               $file

"""

  val s3Client: AmazonS3Client = Clients.s3

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3)

  def file = propNoShrink((s3: S3Temporary, hdfs: HdfsTemporary, data: BigData) => for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    b <- s3.address.execute(s3Client)
    x <- hdfs.path.run(c)
    y <- hdfs.path.run(c)
    _ <- a.put(data.value).execute(s3Client)
    _ <- x.write(data.value).run(c)
    _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(x, b), DownloadMapping(a, y))),
           DistCopyConfiguration(c, s3Client, DistCopyParameters.createDefault(mappersNumber = 1)))
    i <- b.get.execute(s3Client)
    o <- y.read.run(c)
  } yield i -> o ==== data.value -> Some(data.value))

}

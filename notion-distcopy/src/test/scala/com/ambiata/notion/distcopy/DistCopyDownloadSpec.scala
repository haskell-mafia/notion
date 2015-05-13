package com.ambiata.notion.distcopy

import com.ambiata.disorder._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.Arbitraries._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2._
import org.specs2.matcher.Parameters

import scalaz._, Scalaz._

class DistCopyDownloadSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Download files from S3 to HDFS
==============================
  Download file                               $downloadFile
  Download multiple files                     $downloadFiles
  Download nested files                       $nestedFiles
  Handle failure (no source file)             $noSourceFile
  Handle failure (target file exists)         $targetExists

"""

  val s3Client: AmazonS3Client = Clients.s3

  def distCopyConf(c: Configuration, client: AmazonS3Client): DistCopyConfiguration =
    DistCopyConfiguration(
        c
      , client
      , DistCopyParameters.createDefault(mappersNumber = 1)
    )

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3)

  def downloadFile = prop((s3: S3Temporary, hdfs: HdfsTemporary, data: String) => for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    _ <- a.put(data).execute(s3Client)
    _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(a, p))), distCopyConf(c, s3Client))
    e <- Hdfs.exists(p).run(c)
    s <- Hdfs.readContentAsString(p).run(c)
  } yield e -> s ==== true -> data)

  def downloadFiles = prop((s3: S3Temporary, hdfs: HdfsTemporary, data: String) => for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    b <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    o <- hdfs.path.run(c)
    _ <- List(a, b).traverse(_.put(data).execute(s3Client))
    _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(a, p), DownloadMapping(b, o))), distCopyConf(c, s3Client))
    r <- Hdfs.readContentAsString(p).run(c)
    z <- Hdfs.readContentAsString(o).run(c)
  } yield r -> z ==== data -> data)

  def nestedFiles = prop((s3: S3Temporary, hdfs: HdfsTemporary, d: DistinctPair[Ident], data: String) => for {
    c <- ConfigurationTemporary.random.conf
    p <- s3.prefix.execute(s3Client)
    h <- hdfs.path.run(c)
    o = p | d.first.value
    t = p / d.second.value | d.first.value
    x = new Path(h, d.first.value)
    y = new Path(new Path(h, d.second.value), d.first.value)
    _ <- List(o, t).traverse(_.put(data).execute(s3Client))
    _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(o, x), DownloadMapping(t, y))), distCopyConf(c, s3Client))
    r <- Hdfs.readContentAsString(x).run(c)
    z <- Hdfs.readContentAsString(y).run(c)
  } yield r -> z ==== data -> data)

  def noSourceFile = prop((s3: S3Temporary, hdfs: HdfsTemporary) => (for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(a, p))), distCopyConf(c, s3Client))
  } yield ()) must beFail)

  def targetExists = prop((s3: S3Temporary, hdfs: HdfsTemporary, data: String) => (for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    _ <- a.put(data).execute(s3Client)
    _ <- Hdfs.write(p, data).run(c)
    _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(a, p))), distCopyConf(c, s3Client))
  } yield ()) must beFail)
}

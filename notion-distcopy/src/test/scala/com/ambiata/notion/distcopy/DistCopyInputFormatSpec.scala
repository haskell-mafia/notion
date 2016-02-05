package com.ambiata.notion.distcopy

import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.Arbitraries._

import org.specs2._
import org.specs2.matcher.Parameters


class DistCopyInputFormatSpec extends Specification with ScalaCheck { def is = s2"""

 Calculate Workloads correctly based on file size
 ================================================

 calculate upload mappings                $upload
 calculate download mappings              $download ${tag("aws")}

"""
  val s3Client = Clients.s3

  def upload = prop((s3: S3Temporary, hdfs: HdfsTemporary) => for {
    q <- ConfigurationTemporary.random.conf
    a <- hdfs.path.run(q)
    b <- hdfs.path.run(q)
    c <- hdfs.path.run(q)
    s <- s3.address.execute(s3Client)
    _ <- a.write("a").run(q)
    _ <- b.write("bbbbbbb").run(q)
    _ <- c.write("c").run(q)
    r <- DistCopyInputFormat.calc(
      Mappings(Vector(
          UploadMapping(a.toHPath, s)
        , UploadMapping(b.toHPath, s)
        , UploadMapping(c.toHPath, s)
      )), 2, s3Client, q)
  } yield r ==== Workloads(Vector(Workload(Vector(1)), Workload(Vector(0, 2)))))

  def download = prop((s3: S3Temporary, hdfs: HdfsTemporary) => for {
    q <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    b <- s3.address.execute(s3Client)
    c <- s3.address.execute(s3Client)
    h <- hdfs.path.run(q)
    _ <- a.put("aaaaaaaaaaaaaaaaaa").execute(s3Client)
    _ <- b.put("bbbbb").execute(s3Client)
    _ <- c.put("c").execute(s3Client)
    r <- DistCopyInputFormat.calc(
      Mappings(Vector(
          DownloadMapping(a, h.toHPath)
        , DownloadMapping(b, h.toHPath)
        , DownloadMapping(c, h.toHPath)
      )), 2, s3Client, q)
  } yield r ==== Workloads(Vector(Workload(Vector(0)), Workload(Vector(1, 2)))))

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 10)

}

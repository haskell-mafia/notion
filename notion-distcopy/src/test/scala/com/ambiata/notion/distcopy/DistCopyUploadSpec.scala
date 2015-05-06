package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.disorder.Ident
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.distcopy.Arbitraries._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3._
import com.ambiata.notion.core._
import com.ambiata.saws.testing.Arbitraries._
import MemoryConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.specs2._
import org.specs2.matcher.Parameters
import scalaz._, Scalaz._
import org.scalacheck._, Arbitrary._, Gen._

class DistCopyUploadSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Upload files from HDFS to S3
============================
  Upload file                               $uploadFile
  Upload multiple files                     $multipleFiles
  Upload a directory                        $directory
  Handle failure (no source file)           $noSourceFile
  Handle failure (target file exists)       $targetExists

"""
  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3)

  val s3Client: AmazonS3Client = Clients.s3

  def distCopyConf(c: Configuration, client: AmazonS3Client): DistCopyConfiguration =
    DistCopyConfiguration(
        c
      , client
      , 1
      , 1
      , 10.mb
      , 10.mb
      , 100.mb
    )

  def uploadFile = propNoShrink((s3: S3Temporary, hdfs: HdfsTemporary, data: BigData) => for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    _ <- Hdfs.write(p, data.value).run(c)
    _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(p, a))), distCopyConf(c, s3Client))
    r <- a.get.execute(s3Client)
  } yield r ==== data.value)

  def multipleFiles = prop((s3: S3Temporary, hdfs: HdfsTemporary, data: String) => for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    b <- s3.address.execute(s3Client)
    x <- hdfs.path.run(c)
    y <- hdfs.path.run(c)
    _ <- List(x, y).traverse(Hdfs.write(_, data).run(c))
    _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(x, a), UploadMapping(y, b))), distCopyConf(c, s3Client))
    r <- a.get.execute(s3Client)
    z <- b.get.execute(s3Client)
  } yield r -> z ==== data -> data)

  def directory = prop((s3: S3Temporary, hdfs: HdfsTemporary, file1: FilePath, files1: List[FilePath], data: String) => for {
    c  <- ConfigurationTemporary.random.conf
    io =  LocationIO(c, s3Client)
    a  <- s3.address.execute(s3Client)
    to =  S3Location(a.bucket, a.key)
    files = file1 +: files1
    d  <- hdfs.path.run(c)
    _  <- files.map(f => new Path(d, f.path)).traverse(Hdfs.write(_, data).run(c))
    _  <- DistCopy.uploadDirectory(HdfsLocation(d.toString), to, io)
    ks <- io.listS3(to)
    ds <- ks.traverse(io.readUtf8)
  } yield ds ==== files.as(data))

  def noSourceFile = propNoShrink((s3: S3Temporary, hdfs: HdfsTemporary) => (for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(p, a))), distCopyConf(c, s3Client))
  } yield ()) must beFail)

  def targetExists = propNoShrink((s3: S3Temporary, hdfs: HdfsTemporary) => (for {
    c <- ConfigurationTemporary.random.conf
    a <- s3.address.execute(s3Client)
    p <- hdfs.path.run(c)
    _ <- Hdfs.write(p, "").run(c)
    _ <- a.put("").execute(s3Client)
    _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(p, a))), distCopyConf(c, s3Client))
  } yield ()) must beFail)

  implicit def ArbitraryFilePath: Arbitrary[FilePath] =
    Arbitrary {
      for {
        n  <- Gen.choose(1, 3)
        fs <- Gen.listOfN(n, Gen.identifier).map(fs => FilePath.unsafe(fs.mkString("/")))
      } yield fs
    }


}

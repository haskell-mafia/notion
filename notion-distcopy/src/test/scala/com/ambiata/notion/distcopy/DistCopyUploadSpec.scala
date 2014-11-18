package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.distcopy.Arbitraries._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.TemporaryS3._
import MemoryConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalacheck._, Arbitrary._

import org.specs2._
import org.specs2.matcher.Parameters

class DistCopyUploadSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Upload files from HDFS to S3
============================
  Upload file                               $uploadFile
  Upload multiple files                     $multipleFiles
  Handle failure (no source file)           $noSourceFile
  Handle failure (target file exists)       $targetExists

"""

  def withConf[A](f: Configuration => ResultTIO[A]): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    val c = new Configuration()
    c.set("hadoop.tmp.dir", dir.path)
    f(c)
  }
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

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3)

  def uploadFile = propNoShrink((data: BigData) => {
    withConf[(Boolean, String)](conf =>
      withFilePath(file =>
        withS3Address(address => for {
          _ <- Hdfs.writeWith(new Path(file.path), f => Streams.write(f, data.value, "UTF-8")).run(conf)
          _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(new Path(file.path), address))), distCopyConf(conf, s3Client))
          e <- address.exists.executeT(s3Client)
          d <- address.get.executeT(s3Client)
        } yield e -> d)))
  } must beOkValue(true -> data.value))


  def multipleFiles = prop((data: String) => {
    withConf(conf =>
      withFilePath(one =>
        withFilePath(two =>
          withS3Address(s3one =>
            withS3Address(s3two =>
              for {
                _ <- Hdfs.writeWith(new Path(one.path), f => Streams.write(f, data, "UTF-8")).run(conf)
                _ <- Hdfs.writeWith(new Path(two.path), f => Streams.write(f, data, "UTF-8")).run(conf)
                _ <- DistCopyJob.run(
                  Mappings(Vector(
                      UploadMapping(new Path(one.path), s3one)
                    , UploadMapping(new Path(two.path), s3two)
                  )), distCopyConf(conf, s3Client))
                e1 <- s3one.exists.executeT(s3Client)
                e2 <- s3two.exists.executeT(s3Client)
                d1 <- s3two.get.executeT(s3Client)
                d2 <- s3two.get.executeT(s3Client)
              } yield (e1, e2, d1, d2))))))
  } must beOkValue((true, true, data, data)))

  def noSourceFile = prop((data: BigData) => {
    withConf(conf =>
      withFilePath(file =>
        withS3Address(address =>
          DistCopyJob.run(Mappings(Vector(UploadMapping(new Path(file.path), address))), distCopyConf(conf, s3Client))
        )))
  } must beFail)

  def targetExists = prop((data: BigData) => {
    withConf(conf =>
      withFilePath(file =>
        withS3Address(address => for {
          _ <- Hdfs.writeWith(new Path(file.path), f => Streams.write(f, data.value, "UTF-8")).run(conf)
          _ <- address.put(data.value).executeT(s3Client)
          _ <- DistCopyJob.run(Mappings(Vector(UploadMapping(new Path(file.path), address))), distCopyConf(conf, s3Client))
        } yield ())))
  } must beFail)

}

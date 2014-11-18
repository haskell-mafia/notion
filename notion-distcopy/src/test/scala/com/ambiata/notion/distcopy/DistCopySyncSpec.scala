package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.distcopy.Arbitraries._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.TemporaryS3._
import MemoryConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2._
import org.specs2.matcher.Parameters

class DistCopySyncSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Syncing files between S3 and HDFS
=================================

  Upload and Download file                               $file

"""

  def withConf[A](f: Configuration => ResultTIO[A]): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    val c = new Configuration()
    c.set("hadoop.tmp.dir", dir.path)
    f(c)
  }

  val s3Client: AmazonS3Client = Clients.s3

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3)

  def file = propNoShrink((data: BigData) => {
    withConf(conf =>
      withFilePath(sourceFile =>
        withFilePath(targetFile =>
          withS3Address(sourceAddress =>
            withS3Address(targetAddress => {
              val sourcePath = new Path(sourceFile.path)
              val targetPath = new Path(targetFile.path)
              for {
                _ <- Hdfs.writeWith(sourcePath, f => Streams.write(f, data.value, "UTF-8")).run(conf)
                _ <- sourceAddress.put(data.value).executeT(s3Client)
                _ <- DistCopyJob.run(
                  Mappings(Vector(
                      UploadMapping(sourcePath, targetAddress)
                    , DownloadMapping(sourceAddress, targetPath)
                  )), DistCopyConfiguration(conf, s3Client, 1, 10.mb, 10.mb, 100.mb))
                s <- targetAddress.get.executeT(s3Client)
                h <- Hdfs.readContentAsString(targetPath).run(conf)
              } yield s -> h })))))
  } must beOkValue(data.value -> data.value))
}

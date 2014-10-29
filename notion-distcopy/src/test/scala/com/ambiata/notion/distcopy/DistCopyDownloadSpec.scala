package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.Clients
import com.ambiata.saws.testing.TemporaryS3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.testing.ResultTIOMatcher._

import org.specs2._
import org.specs2.matcher.Parameters

class DistCopyDownloadSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Download files from S3 to HDFS
==============================
  Download file                               $downloadFile
  Download multiple files                     $downloadFiles
  Handle failure (no source file)             $noSourceFile
  Handle failure (target file exists)         $targetExists

"""

  def withConf[A](f: Configuration => ResultTIO[A]): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    val c = new Configuration()
    c.set("hadoop.tmp.dir", dir.path)
    f(c)
  }

  val s3Client: AmazonS3Client = Clients.s3

  override implicit def defaultParameters: Parameters =
    new Parameters(minTestsOk = 3)

  def downloadFile = prop((data: String) => {
    withConf(conf =>
      withS3Address(s3 =>
        withFilePath( hdfs => {
          val path = new Path(hdfs.path)
          for {
            _ <- s3.put(data).executeT(s3Client)
            _ <- DistCopyJob.run(conf, s3Client, Mappings(Vector(DownloadMapping(s3, path))), 1)
            e <- Hdfs.exists(path).run(conf)
            s <- Hdfs.readContentAsString(path).run(conf)
          } yield e -> s })))
  } must beOkValue((true, data)))

  def downloadFiles = prop((data: String) => {
    withConf(conf =>
      withS3Address(one =>
        withS3Address(two =>
          withFilePath(hdfsOne =>
            withFilePath(hdfsTwo => {
              val pathOne = new Path(hdfsOne.path)
              val pathTwo = new Path(hdfsTwo.path)
              for {
                _ <- one.put(data).executeT(s3Client)
                _ <- two.put(data).executeT(s3Client)
                _  <- DistCopyJob.run(conf, s3Client, Mappings(Vector(DownloadMapping(one, pathOne), DownloadMapping(two, pathTwo))), 1)
                e1 <- Hdfs.exists(pathOne).run(conf)
                e2 <- Hdfs.exists(pathTwo).run(conf)
                s1 <- Hdfs.readContentAsString(pathOne).run(conf)
                s2 <- Hdfs.readContentAsString(pathTwo).run(conf)
              } yield (e1, e2, s1, s2) })))))
  } must beOkValue((true, true, data, data)))

  def noSourceFile = {
    withConf(conf =>
      withS3Address(s3 =>
        withFilePath(hdfs => {
          val path = new Path(hdfs.path)
          DistCopyJob.run(conf, s3Client, Mappings(Vector(DownloadMapping(s3, path))), 1)
        })))
  } must beFail

  def targetExists = prop((data: String) => {
    withConf(conf =>
      withS3Address(s3 =>
        withFilePath( hdfs => {
          val path = new Path(hdfs.path)
          for {
            _ <- s3.put(data).executeT(s3Client)
            _ <- Hdfs.writeWith(path, f => ResultT.safe(f.write(0))).run(conf)
            _ <- DistCopyJob.run(conf, s3Client, Mappings(Vector(DownloadMapping(s3, path))), 1)
          } yield () })))
  } must beFail)

}

package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.TemporaryS3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.io.TemporaryDirPath._
import com.ambiata.mundane.testing.ResultTIOMatcher._

import org.specs2._
import org.specs2.matcher.Parameters

class DistCopyDownloadSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Syncing files between S3 and HDFS
=================================
  Download prefix with runDownload            $downloadPrefix
  Download file                               $downloadFile
  Download multiple files                     $downloadFiles
  Handle failure (no source file)             $noSourceFile
  Handle failure (target file exists)         $targetExists

"""
  val conf: Configuration = new Configuration()

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
      withS3Address(s3 => for {
      _ <- s3.put(data).executeT(s3Client)
      s <- s3.size.executeT(s3Client)
      r <- withFilePath( hdfs => {
        val path = new Path(hdfs.path)
        for {
          _ <- DistCopyDownloadJob.run(conf, DownloadMappings(Vector(DownloadMapping(SizedS3Address(s3, s), path))), 1)
          e <- Hdfs.exists(path).run(conf)
          s <- Hdfs.readContentAsString(path).run(conf)
        } yield e -> s
      })
    } yield r))
  } must beOkValue((true, data)))

  def downloadFiles = prop((data: String) => {
    withConf(conf =>
      withS3Address(one => for {
        _ <- one.put(data).executeT(s3Client)
        s <- one.size.executeT(s3Client)
        r <- withS3Address(two => for {
          _ <- two.put(data).executeT(s3Client)
          t <- two.size.executeT(s3Client)
          r <- withFilePath(hdfsOne => {
            val pathOne = new Path(hdfsOne.path)
            withFilePath(hdfsTwo => {
              val pathTwo = new Path(hdfsTwo.path)
              for {
                _  <- DistCopyDownloadJob.run(conf, DownloadMappings(Vector(DownloadMapping(SizedS3Address(one, s), pathOne), DownloadMapping(SizedS3Address(two, t), pathTwo))), 1)
                e1 <- Hdfs.exists(pathOne).run(conf)
                e2 <- Hdfs.exists(pathTwo).run(conf)
                s1 <- Hdfs.readContentAsString(pathOne).run(conf)
                s2 <- Hdfs.readContentAsString(pathTwo).run(conf)
              } yield (e1, e2, s1, s2)
            })
          })
        } yield r)
      } yield r))
  } must beOkValue((true, true, data, data)))

  def downloadPrefix = prop((data: String) => {
    withConf(conf =>
      withS3Prefix(s3 => for {
        _ <- (s3 | "foo").put(data).executeT(s3Client)
        _ <- (s3 / "foos" | "bar").put(data).executeT(s3Client)
        e <- withDirPath(hdfs => for {
          f    <- DownloadMapping.fromS3Address(s3 | "foo", new Path(hdfs.path + "/foo"), s3Client)
          ff   <- DownloadMapping.fromS3Address(s3 / "foos" | "bar", new Path(hdfs.path + "/foos/bar"), s3Client)
          mappings = DownloadMappings(Vector(f, ff))
          _    <- DistCopyDownloadJob.run(conf, mappings, 1)
          foo  <- Files.exists(hdfs </> FilePath("foo"))
          bar  <- Files.exists(hdfs </> DirPath("foos") </> FilePath("bar"))
        } yield (foo, bar))
      } yield e))
  } must beOkValue((true, true)))

  def noSourceFile = {
    withConf(conf =>
      withS3Address(s3 => for {
        _ <- withFilePath( hdfs => {
          val path = new Path(hdfs.path)
          for {
            _ <- DistCopyDownloadJob.run(conf, DownloadMappings(Vector(DownloadMapping(SizedS3Address(s3, 0), path))), 1)
          } yield ()
        })
      } yield ()))
  } must beFail

  def targetExists = prop((data: String) => {
    withConf(conf =>
      withS3Address(s3 => for {
        _ <- s3.put(data).executeT(s3Client)
        s <- s3.size.executeT(s3Client)
        _ <- withFilePath( hdfs => {
          val path = new Path(hdfs.path)
          for {
            _ <- Hdfs.writeWith(path, f => ResultT.safe(f.write(0))).run(conf)
            _ <- DistCopyDownloadJob.run(conf, DownloadMappings(Vector(DownloadMapping(SizedS3Address(s3, s), path))), 1)
          } yield ()
        })
      } yield ()))
  } must beFail)

}

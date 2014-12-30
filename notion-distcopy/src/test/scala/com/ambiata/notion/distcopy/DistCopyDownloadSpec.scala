package com.ambiata.notion.distcopy

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.TemporaryS3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.io.TemporaryDirPath._
import com.ambiata.mundane.testing.RIOMatcher._
import MemoryConversions._

import org.specs2._
import org.specs2.matcher.Parameters

class DistCopyDownloadSpec extends Specification with ScalaCheck { def is = section("aws") ^ s2"""

Download files from S3 to HDFS
==============================
  Download file                               $downloadFile
  Download multiple files                     $downloadFiles
  Download nested files                       $nestedFiles
  Handle failure (no source file)             $noSourceFile
  Handle failure (target file exists)         $targetExists

"""

  def withConf[A](f: Configuration => RIO[A]): RIO[A] = TemporaryDirPath.withDirPath { dir =>
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

  def downloadFile = prop((data: String) => {
    withConf(conf =>
      withS3Address(s3 =>
        withFilePath( hdfs => {
          val path = new Path(hdfs.path)
          for {
            _ <- s3.put(data).execute(s3Client)
            _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(s3, path))), distCopyConf(conf, s3Client))
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
                _ <- one.put(data).execute(s3Client)
                _ <- two.put(data).execute(s3Client)
                _  <- DistCopyJob.run(Mappings(Vector(DownloadMapping(one, pathOne), DownloadMapping(two, pathTwo))), distCopyConf(conf, s3Client))
                e1 <- Hdfs.exists(pathOne).run(conf)
                e2 <- Hdfs.exists(pathTwo).run(conf)
                s1 <- Hdfs.readContentAsString(pathOne).run(conf)
                s2 <- Hdfs.readContentAsString(pathTwo).run(conf)
              } yield (e1, e2, s1, s2) })))))
  } must beOkValue((true, true, data, data)))

  def nestedFiles = prop((data: String) => {
    withConf(conf =>
      withS3Prefix(prefix =>
        withDirPath(dir => {
          val one = prefix / "foos" | "foo"
          val two = prefix / "foos" | "bar"
          val three = prefix / "foos" / "bars" | "bar"
          val pathOne = new Path((dir </> DirPath.unsafe("foos") </> FilePath.unsafe("foo")).path)
          val pathTwo = new Path((dir </> DirPath.unsafe("foos") </> FilePath.unsafe("bar")).path)
          val pathThree = new Path((dir </> DirPath.unsafe("foos") </> DirPath.unsafe("bars") </> FilePath.unsafe("foo")).path)
          for {
            _  <- one.put(data).execute(s3Client)
            _  <- two.put(data).execute(s3Client)
            _  <- three.put(data).execute(s3Client)
            _  <- DistCopyJob.run(Mappings(Vector(DownloadMapping(one, pathOne), DownloadMapping(two, pathTwo), DownloadMapping(three, pathThree))), distCopyConf(conf, s3Client))
            e1 <- Hdfs.exists(pathOne).run(conf)
            e2 <- Hdfs.exists(pathTwo).run(conf)
            e3 <- Hdfs.exists(pathThree).run(conf)
            s1 <- Hdfs.readContentAsString(pathOne).run(conf)
            s2 <- Hdfs.readContentAsString(pathTwo).run(conf)
            s3 <- Hdfs.readContentAsString(pathThree).run(conf)
          } yield (e1, e2, e3, s1, s2, s3) })))
  } must beOkValue((true, true, true, data, data, data)))

  def noSourceFile = {
    withConf(conf =>
      withS3Address(s3 =>
        withFilePath(hdfs => {
          val path = new Path(hdfs.path)
          DistCopyJob.run(Mappings(Vector(DownloadMapping(s3, path))), distCopyConf(conf, s3Client))
        })))
  } must beFail

  def targetExists = prop((data: String) => {
    withConf(conf =>
      withS3Address(s3 =>
        withFilePath( hdfs => {
          val path = new Path(hdfs.path)
          for {
            _ <- s3.put(data).execute(s3Client)
            _ <- Hdfs.writeWith(path, f => RIO.safe(f.write(0))).run(conf)
            _ <- DistCopyJob.run(Mappings(Vector(DownloadMapping(s3, path))), distCopyConf(conf, s3Client))
          } yield () })))
  } must beFail)

}

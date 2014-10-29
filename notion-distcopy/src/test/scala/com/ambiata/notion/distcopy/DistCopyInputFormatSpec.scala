package com.ambiata.notion.distcopy

import com.ambiata.mundane.io._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.S3Address
import com.ambiata.saws.testing.TemporaryS3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.specs2._


class DistCopyInputFormatSpec extends Specification with ScalaCheck { def is = s2"""

 Calculate Workloads correctly based on file size
 ================================================

 calculate upload mappings                $upload
 calculate download mappings              $download

"""

  val conf = new Configuration()
  val s3Client = Clients.s3

  def upload = {
    withFilePath(fileA =>
      withFilePath(fileB =>
        withFilePath(fileC => {
          val pathA = new Path(fileA.path)
          val pathB = new Path(fileB.path)
          val pathC = new Path(fileC.path)
          val s3 = S3Address("","")
          for {
            _ <- Hdfs.writeWith(pathA, f => Streams.write(f, "a", "UTF-8")).run(conf)
            _ <- Hdfs.writeWith(pathB, f => Streams.write(f, "bbbbbb", "UTF-8")).run(conf)
            _ <- Hdfs.writeWith(pathC, f => Streams.write(f, "c", "UTF-8")).run(conf)
            r <- DistCopyInputFormat.calc(
              Mappings(Vector(
                  UploadMapping(pathA, s3)
                , UploadMapping(pathB, s3)
                , UploadMapping(pathC, s3)
              )), 2, s3Client, conf)
          } yield r
        })))
  } must beOkValue(Workloads(Vector(Workload(Vector(2, 0)), Workload(Vector(1)))))


  def download = {
    withS3Address(s3A =>
      withS3Address(s3B =>
        withS3Address(s3C => {
          val path = new Path("foo")
          for {
            _ <- s3A.put("aaaaaaaaaaa").executeT(s3Client)
            _ <- s3B.put("bbbb").executeT(s3Client)
            _ <- s3C.put("c").executeT(s3Client)
            r <- DistCopyInputFormat.calc(
              Mappings(Vector(
                  DownloadMapping(s3A, path)
                , DownloadMapping(s3B, path)
                , DownloadMapping(s3C, path)
              )), 2, s3Client, conf)
          } yield r
        })))
  } must beOkValue(Workloads(Vector(Workload(Vector(2, 1)), Workload(Vector(0)))))

}

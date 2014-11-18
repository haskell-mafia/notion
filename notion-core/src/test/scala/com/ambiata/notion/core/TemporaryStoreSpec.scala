package com.ambiata.notion.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.io.Temporary._
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core.TemporaryStore._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.S3Prefix
import org.apache.hadoop.conf.Configuration
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import scalaz._, Scalaz._

class TemporaryStoreSpec extends Specification { def is = s2"""

 TemporaryStore should clean up its own resources when
 =====================================================

   on the local file system               $localStore
   on hdfs                                $hdfsStore
   on s3                                  $s3Store              ${tag("aws")}

"""
  def s3Store =
    withStore(S3Store(S3Prefix(testBucket, s3TempPath), Clients.s3, uniqueDirPath))

  def hdfsStore =
    withStore(HdfsStore(new Configuration, uniqueDirPath))

  def localStore =
    withStore(PosixStore(uniqueDirPath))

  def withStore(store: Store[ResultTIO]): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryStore.runWithStore(store)(tmpStore => for {
        _   <- tmpStore.utf8.write(Key.unsafe("test"), "")
        dir <- tmpStore.exists(Key.unsafe("test"))
      } yield dir)
      y <- store.exists(Key.unsafe("test"))
    } yield (x,y)) must beOkValue((true,false))

  def runWithStore[A](store: Store[ResultTIO])(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryStore(store).pure[ResultTIO])(tmp => f(tmp.store))

}

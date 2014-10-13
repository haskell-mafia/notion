package com.ambiata.notion.testing

import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.notion.testing.TemporaryStore._
import com.ambiata.saws.core.Clients
import org.apache.hadoop.conf.Configuration
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import scalaz.{Store => _, _}, Scalaz._

class TemporaryStoreSpec extends Specification { def is = s2"""

 TemporaryStore should clean up its own resources when
 =====================================================

   on the local file system               $localStore
   on hdfs                                $hdfsStore
   on s3                                  $s3Store              ${tag("aws")}

"""
  def s3Store =
    withStore(S3Store(testBucket, s3TempDirPath, Clients.s3, createUniquePath))

  def hdfsStore =
    withStore(HdfsStore(new Configuration, createUniquePath))

  def localStore =
    withStore(PosixStore(createUniquePath))

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
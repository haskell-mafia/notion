package com.ambiata.notion.core

import java.util.UUID

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Temporary._
import com.ambiata.notion.core.{TemporaryType => T}
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.S3Prefix
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._, effect._
import scalaz.{Store => _}

case class TemporaryStore(store: Store[RIO]) {
  def clean: RIO[Unit] = for {
    _ <- store.deleteAll(Key.Root)
    _ <- store match {
      case S3Store(_, _, s) =>
        Directories.delete(s)
      case _ =>
        RIO.unit
      }
  } yield ()
}

object TemporaryStore {

  implicit val TemporaryStoreResource: Resource[TemporaryStore] = new Resource[TemporaryStore] {
    def close(temp: TemporaryStore) = temp.clean.run.void // Squelch errors
  }

  def withStore[A](storeType: TemporaryType)(f: Store[RIO] => RIO[A]): RIO[A] = {
    val store = storeType match {
      case T.Posix =>
        PosixStore(uniqueDirPath)
      case T.S3    =>
        S3Store(S3Prefix(testBucket, s3TempPath), Clients.s3, uniqueDirPath)
      case T.Hdfs  =>
        HdfsStore(new Configuration, uniqueDirPath)
    }
    runWithStore(store)(f)
  }

  def runWithStore[A](store: Store[RIO])(f: Store[RIO] => RIO[A]): RIO[A] =
    RIO.using(TemporaryStore(store).pure[RIO])(tmp => f(tmp.store))

  def testBucket: String = Option(System.getenv("AWS_TEST_BUCKET")).getOrElse("ambiata-dev-view")

  def s3TempPath: String = s"tests/temporary-${UUID.randomUUID()}"

}

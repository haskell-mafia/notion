package com.ambiata.notion.core

import java.util.UUID

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core.{TemporaryType => T}
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.S3Prefix
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._, effect._
import scalaz.{Store => _}

case class TemporaryStore(store: Store[ResultTIO]) {
  def clean: ResultT[IO, Unit] = for {
    _ <- store.deleteAll(Key.Root)
    _ <- store match {
      case S3Store(_, _, s) =>
        Directories.delete(s)
      case _ => ResultT.unit[IO]
      }
  } yield ()
}

object TemporaryStore {

  implicit val TemporaryStoreResource: Resource[TemporaryStore] = new Resource[TemporaryStore] {
    def close(temp: TemporaryStore) = temp.clean.run.void // Squelch errors
  }

  def withStore[A](storeType: TemporaryType)(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] = {
    val store = storeType match {
      case T.Posix =>
        PosixStore(createUniquePath)
      case T.S3    =>
        S3Store(S3Prefix(testBucket, s3TempDirPath), Clients.s3, createUniquePath)
      case T.Hdfs  =>
        HdfsStore(new Configuration, createUniquePath)
    }
    runWithStore(store)(f)
  }

  def runWithStore[A](store: Store[ResultTIO])(f: Store[ResultTIO] => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryStore(store).pure[ResultTIO])(tmp => f(tmp.store))

  def createUniquePath: DirPath =
    DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> tempUniquePath

  def testBucket: String = Option(System.getenv("AWS_TEST_BUCKET")).getOrElse("ambiata-dev-view")

  def s3TempDirPath: String =
    s"tests/temporary-${UUID.randomUUID()}"

  def tempUniquePath: DirPath =
    DirPath.unsafe(s"temporary-${UUID.randomUUID()}")
}
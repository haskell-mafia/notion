package com.ambiata.notion
package core

import java.util.UUID
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import TemporaryType._
import io.LocationIO
import com.ambiata.poacher.hdfs.{Hdfs => PoacherHdfs}
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3.{S3Address}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scalaz.effect.Resource
import scalaz.{Store =>_,_}, Scalaz._

/**
 * Temporary locations for Location tests
 */
trait TemporaryLocations {

  /** run some code with a location representing a directory */
  def withLocationDir[A](temporaryType: TemporaryType)(f: Location => ResultTIO[A]): ResultTIO[A] = {
    runWithLocationDir(createLocation(temporaryType))(f)
  }

  /** run some code with a location representing a file */
  def withLocationFile[A](temporaryType: TemporaryType)(f: Location => ResultTIO[A]): ResultTIO[A] =
    runWithLocationFile(createLocation(temporaryType))(f)

  /** create a temporary location for a given type of location: posix, hdfs, s3 */
  def createLocation(temporaryType: TemporaryType): Location = {
    val uniquePath = createUniquePath.path
    temporaryType match {
      case Posix  => LocalLocation(uniquePath)
      case S3     => S3Location(testBucket, uniquePath)
      case Hdfs   => HdfsLocation(uniquePath)
    }
  }

  /** run a function with temporary file which will be removed after usage */
  def runWithLocationFile[A](location: Location)(f: Location => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationFile(location).pure[ResultTIO])(tmp => f(tmp.location))

  /** run a function with temporary directory which will be removed after usage */
  def runWithLocationDir[A](location: Location)(f: Location => ResultTIO[A]): ResultTIO[A] =
    ResultT.using(TemporaryLocationDir(location).pure[ResultTIO])(tmp => f(tmp.location))

  def createUniquePath: DirPath =
    DirPath.unsafe(System.getProperty("java.io.tmpdir", "/tmp")) </> tempUniquePath

  def createLocalLocation: LocalLocation     = LocalLocation(createUniquePath.path)
  def createUniqueS3Location: S3Location     = S3Location(testBucket, createUniquePath.asRelative.path)
  def createUniqueHdfsLocation: HdfsLocation = HdfsLocation(createUniquePath.path)

  def createLocationFile(location: Location): ResultTIO[Unit] =
    saveLocationFile(location, "")

  def saveLocationFile(location: Location, content: String): ResultTIO[Unit] =
    LocationIO(new Configuration, Clients.s3).writeUtf8(location, content)

  def createLocationDir(location: Location): ResultTIO[Unit] = location match {
    case l @ LocalLocation(path)     => Directories.mkdirs(DirPath.unsafe(path))
    case s @ S3Location(bucket, key) => (S3Address(bucket, key) / ".location").put("").executeT(Clients.s3).void
    case h @ HdfsLocation(p)         => PoacherHdfs.mkdir(new Path(p)).void.run(new Configuration)
  }

  def testBucket: String = Option(System.getenv("AWS_TEST_BUCKET")).getOrElse("ambiata-dev-view")

  def testBucketDir: DirPath = DirPath.unsafe(testBucket)

  def s3TempPath: String =
    s"tests/temporary-${UUID.randomUUID()}"

  def tempUniquePath: DirPath =
    DirPath.unsafe(s"temporary-${UUID.randomUUID()}")
}

object TemporaryLocations extends TemporaryLocations


case class TemporaryLocationDir(location: Location) {
  def clean: ResultTIO[Unit] = LocationIO(new Configuration, Clients.s3).deleteAll(location)
}

object TemporaryLocationDir {
  implicit val TemporaryLocationDirResource: Resource[TemporaryLocationDir] = new Resource[TemporaryLocationDir] {
    def close(temp: TemporaryLocationDir) = temp.clean.run.void // Squelch errors
  }
}

case class TemporaryLocationFile(location: Location) {
  def clean: ResultTIO[Unit] = LocationIO(new Configuration, Clients.s3).delete(location)
}

object TemporaryLocationFile {
  implicit val TemporaryLocationFileResource: Resource[TemporaryLocationFile] = new Resource[TemporaryLocationFile] {
    def close(temp: TemporaryLocationFile) = temp.clean.run.void // Squelch errors
  }
}



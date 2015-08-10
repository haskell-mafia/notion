package com.ambiata.notion
package distcopy

import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.core._
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.Arbitraries._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalacheck._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.saws.testing._
import Arbitraries._

class DistCopySpec extends AwsScalaCheckSpec(tests = 5) { def is = s2"""

  A download mapping must not be created if the file already exists
    if no file exists $downloadMappingIsSome
    if a file exists  $downloadMappingIsNone

  An upload mapping must not be created if the file already exists
    if no file exists $uploadMappingIsSome
    if a file exists  $uploadMappingIsFail

  A download mapping must contain
   the full path of the file to copy $createDownloadMappingForADirectory

  An upload mapping must contain
    the path of the file to copy relative to the from dir $createUploadMappingForADirectory

  Job should return successfully
    with an empty stats object if the mappings is empty $emptyMappings
"""

  def downloadMappingIsSome = prop { (s3Temp: S3Temporary, hdfsTemp: HdfsTemporary) =>
    for {
      path    <- hdfsTemp.path.run(configuration)
      address <- s3Temp.address.execute(s3Client)
      result  <- DistCopy.createDownloadMapping(HdfsLocation(path.toString), locationIO)(address)
    } yield result must beSome
  }

  def downloadMappingIsNone = prop { (s3Temp: S3Temporary, hdfsTemp: HdfsTemporary) =>
    for {
      path    <- hdfsTemp.path.run(configuration)
      address <- s3Temp.address.execute(s3Client)
      _       <- Hdfs.write(new Path(path, FilePath.unsafe(address.key).path), "old lines").run(configuration)
      result  <- DistCopy.createDownloadMapping(HdfsLocation(path.toString), locationIO)(address)
    } yield result must beNone
  }

  def uploadMappingIsSome = prop { (s3Temp: S3Temporary, hdfsTemp: HdfsTemporary) =>
    for {
      path    <- hdfsTemp.path.run(configuration)
      address <- s3Temp.pattern.execute(s3Client)
      result  <- DistCopy.createUploadMapping(S3Location(address.bucket, address.unknown), HdfsLocation(path.getParent.toUri.getPath), locationIO)(path)
    } yield result must beSome
  }

  def uploadMappingIsFail = prop { (s3Temp: S3Temporary, hdfsTemp: HdfsTemporary) =>
    val result = for {
      path    <- hdfsTemp.path.run(configuration)
      prefix  <- s3Temp.prefix.execute(s3Client)
      address =  S3Address(prefix.bucket, (DirPath.unsafe(prefix.prefix) </> FilePath.unsafe(path.getName)).path)
      _       <- address.put("old lines").execute(s3Client)
      _       <- DistCopy.createUploadMapping(S3Location(prefix.bucket, prefix.prefix), HdfsLocation(path.getParent.toUri.getPath), locationIO)(path)
    } yield ()

    result must beFail
  }

  def createDownloadMappingForADirectory = prop { (s3Temp: S3Temporary, hdfsTemp: HdfsTemporary, filePath: FilePath) =>
    for {
      dir      <- hdfsTemp.path.run(configuration)
      prefix   <- s3Temp.prefix.execute(s3Client)
      address  =  prefix | filePath.path
      result   <- DistCopy.createDownloadMapping(HdfsLocation(dir.toString), locationIO)(address)
    } yield result must beSome(DownloadMapping(address, new Path(dir, address.key)))
  }

  def createUploadMappingForADirectory = prop { (s3Temp: S3Temporary, hdfsTemp: HdfsTemporary, filePath: FilePath) =>
    for {
      dir     <- hdfsTemp.path.run(configuration)
      path    =  new Path(dir, filePath.path)
      address <- s3Temp.pattern.execute(s3Client)
      result  <- DistCopy.createUploadMapping(S3Location(address.bucket, address.unknown), HdfsLocation(dir.toUri.getPath), locationIO)(path)
    } yield result must beSome(UploadMapping(path, S3Address(address.bucket, (DirPath.unsafe(address.unknown) </> filePath).path)))
  }

  def emptyMappings =
    for {
      c <- ConfigurationTemporary.random.conf
      d = DistCopyConfiguration(c, Clients.s3, DistCopyParameters.createDefault(mappersNumber = 1))
      r <- DistCopyJob.run(Mappings(Vector.empty), d)
    } yield r must_==(DistCopyStats.empty)

  /**
   * HELPERS
   */

  def locationIO = LocationIO(new Configuration, Clients.s3)
  def configuration = locationIO.configuration
  val s3Client = locationIO.s3Client

  implicit def HdfsTemporaryArbitrary: Arbitrary[HdfsTemporary] = Arbitrary(for {
    i <- Gen.choose(1, 5)
    a <- Gen.listOfN(i, Gen.identifier)
    z = a.mkString("/")
    f <- Gen.oneOf("", "/")
  } yield HdfsTemporary(s"temporary-${java.util.UUID.randomUUID().toString}/" + z + f))
}

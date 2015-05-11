package com.ambiata.notion
package distcopy

import com.ambiata.disorder.List10
import com.ambiata.mundane.io.DirPath
import com.ambiata.notion.core._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.testing.AwsScalaCheckSpec
import org.apache.hadoop.conf.Configuration
import SynchronizedInputsOutputs._
import org.scalacheck.{Gen, Arbitrary}
import org.specs2.matcher._
import Arbitrary._
import com.ambiata.notion.core.Arbitraries._
import org.apache.hadoop.fs.Path

class SynchronizedInputsOutputsSpec extends AwsScalaCheckSpec(tests = 5) with DisjunctionMatchers { def is = s2"""

 A configuration is a Cluster configuration if the file system scheme is hdfs $clusterConfiguration

 When a sync directory is used
  the sync dir must be in a location that is compatible with the configuration in LocationIO   $validConfiguration

  if the sync dir is not defined then
    if the configuration is local, then all input/output locations must be local
    if the configuration is hdfs then all input/output locations must be hdfs                  $syncDirNotDefined

  if the sync dir is defined on hdfs
    the configuration must be on hdfs
    at least one input or output location is not hdfs                                          $syncDirOnHdfs

  if the sync dir is defined on local
    the configuration must be local
    at least one input or output location is not local and no location is hdfs                 $syncDirOnLocal

  the sync dir can not be defined on S3                                                        $syncDirOnS3

 The synchronized location for a location on S3 depends on the sync directory location.
   The authorised combinations for the input location and the execution location are
     Local / Local => LocalNoSync
     Hdfs  / Hdfs  => HdfsNoSync
     Local / Hdfs  => LocalHdfsSync
     S3    / Hdfs  => S3HdfsSync
     S3    / Local => S3LocalSync                                                              $createSyncLocation

   The sync paths must be correct
     when the source location is local
        if there is a sync dir                                                                 $syncLocalPathWithSyncDir
        if there is no sync dir                                                                $syncLocalPathWithNoSyncDir
     when the source location is hdfs
        if there is a sync dir                                                                 $syncHdfsPathWithSyncDir
        if there is no sync dir                                                                $syncHdfsPathWithNoSyncDir
     when the source location is s3
        if there is a sync dir                                                                 $syncS3PathWithSyncDir
        if there is no sync dir                                                                $syncS3PathWithNoSyncDir
"""

  def clusterConfiguration = prop { scheme: Scheme =>
    isClusterConfiguration(createConfiguration(scheme)).iff(scheme.s == "hdfs")
  }

  def validConfiguration = prop { (syncDirLocation: Location, s3: S3Location, local: LocalLocation, configuration: Configuration) =>
    val valid1 =
      isClusterConfiguration(configuration) && isHdfsLocation(syncDirLocation) ||
      isLocalConfiguration(configuration)   && isLocalLocation(syncDirLocation)

    // create a list of inputs which require a sync directory
    val inputs =
      if (isClusterConfiguration(configuration)) List(local)
      else List(s3)

    val valid2 = validateSyncDir(Some(syncDirLocation), inputs, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def syncDirNotDefined = prop { (locations: List10[Location], configuration: Configuration) =>
    val valid1 =
        isClusterConfiguration(configuration) && !locations.value.exists(l => !isHdfsLocation(l)) ||
        isLocalConfiguration(configuration)   && !locations.value.exists(l => !isLocalLocation(l))

    val valid2 = validateSyncDir(None, locations.value, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def syncDirOnHdfs = prop { (syncDir: HdfsLocation, locations: List10[Location], configuration: Configuration) =>
    val valid1 =
      isClusterConfiguration(configuration) && locations.value.exists(l => !isHdfsLocation(l))

    val valid2 = validateSyncDir(Some(syncDir), locations.value, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def syncDirOnLocal = prop { (syncDir: LocalLocation, locations: List10[Location], configuration: Configuration) =>
    val valid1 =
      isLocalConfiguration(configuration) && locations.value.exists(l => !isLocalLocation(l))

    val valid2 = validateSyncDir(Some(syncDir), locations.value, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def syncDirOnS3 = prop { (syncDir: S3Location, locations: List10[Location], configuration: Configuration) =>
    validateSyncDir(Some(syncDir), locations.value, new LocationIO(configuration, Clients.s3)) must be_-\/
  }

  def createSyncLocation = prop { (location: Location, syncDirLocation: Option[ExecutionLocation]) =>
    val valid1 =
      !syncDirLocation.isDefined && !isS3Location(location) ||
      syncDirLocation.isDefined  && syncDirLocation.exists(isHdfs) ||
      syncDirLocation.isDefined  && syncDirLocation.exists(isLocal) && !isHdfsLocation(location)

    val syncLocation = createSynchronizedLocation(syncDirLocation, location)
    val valid2 = syncLocation.isRight

    val syncDirIsHdfs = syncDirLocation.exists(_.fold(_ => true, _ => false))
    val isHdfsNoSync = isHdfsLocation(location) && syncDirIsHdfs

    valid1 ==> valid2 :| "v1 ==> v2" &&
    valid2 ==> valid1 :| "v2 ==> v1"
  }

  def syncLocalPathWithSyncDir = prop { (location: LocalLocation, syncDir: ExecutionLocation) =>
    val syncLocation = createSynchronizedLocation(Some(syncDir), location).toOption.get
    val syncDirIsHdfs = syncDir.fold(_ => true, _ => false)

    (syncLocation.path ==== new Path(location.path)).when(!syncDirIsHdfs) and
    (syncLocation.path ==== new Path(syncDir.path.toUri.getPath, DirPath.unsafe(location.path).asRelative.path)).when(syncDirIsHdfs)
  }

  def syncLocalPathWithNoSyncDir = prop { location: LocalLocation =>
    val syncLocation = createSynchronizedLocation(None, location).toOption.get
    syncLocation.path ==== new Path(location.path)
  }

  def syncHdfsPathWithSyncDir = prop { (location: HdfsLocation, syncDir: ExecutionLocation) =>
     val syncLocation = createSynchronizedLocation(Some(syncDir), location).toOption
     val syncDirIsHdfs = syncDir.fold(_ => true, _ => false)

     (syncLocation ==== None).when(!syncDirIsHdfs) and
     (syncLocation.map(_.path) ==== Some(new Path(location.path))).when(syncDirIsHdfs)
   }

  def syncHdfsPathWithNoSyncDir = prop { location: HdfsLocation =>
    val syncLocation = createSynchronizedLocation(None, location).toOption.get
    syncLocation.path ==== new Path(location.path)
  }

  def syncS3PathWithSyncDir = prop { (location: S3Location, syncDir: ExecutionLocation) =>
    val syncLocation = createSynchronizedLocation(Some(syncDir), location).toOption.get
    val syncDirIsHdfs = syncDir.fold(_ => true, _ => false)

    syncLocation.path ==== new Path(syncDir.path.toUri.getPath, DirPath.unsafe(location.key).path)
  }

  def syncS3PathWithNoSyncDir = prop { location: S3Location =>
    val syncLocation = createSynchronizedLocation(None, location).toOption
    syncLocation ==== None
  }

  /**
   * HELPERS
   */

  implicit def ArbitraryExecutionLocation: Arbitrary[ExecutionLocation] =
    Arbitrary(arbitrary[Location].map(ExecutionLocation.fromLocation).filter(_.isDefined).map(_.get))

  def createConfiguration: Scheme => Configuration =  { scheme: Scheme =>
    val c = new Configuration()
    c.set("fs.defaultFS", scheme.s+":///")
    c
  }

  case class Scheme(s: String)

  implicit def ArbitraryConfiguration: Arbitrary[Configuration] =
    Arbitrary(arbitrary[Scheme].map(createConfiguration))

  implicit def ArbitraryScheme: Arbitrary[Scheme] =
    Arbitrary(Gen.oneOf("hdfs", "file").map(Scheme))

  def isHdfs(location: ExecutionLocation): Boolean =
    location.fold(_ => true, _ => false)

  def isLocal(location: ExecutionLocation): Boolean =
    location.fold(_ => false, _ => true)

  def isS3Location(location: Location): Boolean =
    location match {
      case S3Location(_,_)  => true
      case HdfsLocation(_)  => false
      case LocalLocation(_) => false
    }

  val locationIO = new LocationIO(new Configuration, Clients.s3)
}

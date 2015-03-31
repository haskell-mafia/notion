package com.ambiata.notion
package distcopy

import com.ambiata.disorder.List10
import com.ambiata.notion.core._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.testing.AwsScalaCheckSpec
import org.apache.hadoop.conf.Configuration
import SynchronizedInputsOutputs._
import org.scalacheck.{Gen, Arbitrary}
import org.specs2.matcher._
import Arbitrary._
import com.ambiata.notion.core.Arbitraries._

class SynchronizedInputsOutputsSpec extends AwsScalaCheckSpec(tests = 5) with DisjunctionMatchers { def is = s2"""

 A configuration is a Cluster configuration if the file system scheme is hdfs $clusterConfiguration

 When a shadow directory is used
  the shadow dir must be in a location that is compatible with the configuration in LocationIO $validConfiguration

  if the shadow dir is not defined then
    if the configuration is local, then all input/output locations must be local
    if the configuration is hdfs then all input/output locations must be hdfs                  $shadowDirNotDefined

  if the shadow dir is defined on hdfs
    the configuration must be on hdfs
    at least one input or output location is not hdfs                                          $shadowDirOnHdfs

  if the shadow dir is defined on local
    the configuration must be local
    at least one input or output location is not local and no location is hdfs                 $shadowDirOnLocal

  the shadow dir can not be defined on S3                                                      $shadowDirOnS3

 The synchronized location for a location on S3 depends on the shadow directory location.
   The authorised combinations for the input location and the execution location are
     Local / Local => LocalNoSync
     Hdfs  / Hdfs  => HdfsNoSync
     Local / Hdfs  => LocalHdfsSync
     S3    / Hdfs  => S3HdfsSync
     S3    / Local => S3LocalSync                                                              $createSyncLocation

"""

  def clusterConfiguration = prop { scheme: Scheme =>
    isClusterConfiguration(createConfiguration(scheme)).iff(scheme.s == "hdfs")
  }

  def validConfiguration = prop { (shadowDirLocation: Location, s3: S3Location, local: LocalLocation, configuration: Configuration) =>
    val valid1 =
      isClusterConfiguration(configuration) && isHdfsLocation(shadowDirLocation) ||
      isLocalConfiguration(configuration)   && isLocalLocation(shadowDirLocation)

    // create a list of inputs which require a shadow directory
    val inputs =
      if (isClusterConfiguration(configuration)) List(local)
      else List(s3)

    val valid2 = validateShadowDir(Some(shadowDirLocation), inputs, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def shadowDirNotDefined = prop { (locations: List10[Location], configuration: Configuration) =>
    val valid1 =
        isClusterConfiguration(configuration) && !locations.value.exists(l => !isHdfsLocation(l)) ||
        isLocalConfiguration(configuration)   && !locations.value.exists(l => !isLocalLocation(l))

    val valid2 = validateShadowDir(None, locations.value, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def shadowDirOnHdfs = prop { (shadowDir: HdfsLocation, locations: List10[Location], configuration: Configuration) =>
    val valid1 =
      isClusterConfiguration(configuration) && locations.value.exists(l => !isHdfsLocation(l))

    val valid2 = validateShadowDir(Some(shadowDir), locations.value, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def shadowDirOnLocal = prop { (shadowDir: LocalLocation, locations: List10[Location], configuration: Configuration) =>
    val valid1 =
      isLocalConfiguration(configuration) && locations.value.exists(l => !isLocalLocation(l))

    val valid2 = validateShadowDir(Some(shadowDir), locations.value, new LocationIO(configuration, Clients.s3)).isRight

    valid1 ==== valid2
  }

  def shadowDirOnS3 = prop { (shadowDir: S3Location, locations: List10[Location], configuration: Configuration) =>
    validateShadowDir(Some(shadowDir), locations.value, new LocationIO(configuration, Clients.s3)) must be_-\/
  }

  def createSyncLocation = prop { (location: Location, shadowDirLocation: Option[ExecutionLocation]) =>
    val valid1 =
      !shadowDirLocation.isDefined && !isS3Location(location) ||
      shadowDirLocation.isDefined  && shadowDirLocation.exists(isHdfs) ||
      shadowDirLocation.isDefined  && shadowDirLocation.exists(isLocal) && !isHdfsLocation(location)

    val valid2 = createSynchronizedLocation(shadowDirLocation, location).isRight

    valid1 ==> valid2 :| "v1 ==> v2" &&
    valid2 ==> valid1 :| "v2 ==> v1"
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

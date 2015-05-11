package com.ambiata.notion
package distcopy

import DistCopy._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.saws.s3.S3Pattern
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import scalaz._, Scalaz._
import SynchronizedLocation._

/**
 * Those functions can be used to prepare input and output files for MapReduce applications
 * by downloading input files to the cluster and uploading results to S3
 *
 * The usage is:
 *
 *  1. validateSyncDir to make sure it is valid to use a sync directory given the location of input/output files + the configuration
 *  2. createSynchronizedLocations for input files
 *  3. synchronizeInputs
 *  4. run the application using the synchronized input paths or locations
 */
object SynchronizedInputsOutputs {

  /**
   * Validate the sync directory location
   *
   *  - the sync dir must be in a location that is compatible with the configuration in LocationIO
   *  - if the sync dir is not defined then
   *     - if the configuration is local, then all input/output locations must be local
   *     - if the configuration is hdfs then all input/output locations must be hdfs
   *  - if the sync dir is defined on hdfs
   *     - the configuration must be on hdfs
   *     - at least one input or output location is not hdfs
   *  - if the sync dir is defined on local
   *     - the configuration must be local
   *     - at least one input or output location is not local and no location is hdfs
   *  - the sync dir can not be defined on S3
   */
  def validateSyncDir(syncDir: Option[Location], locations: List[Location], locationIO: LocationIO): String \/ Option[ExecutionLocation] = {
    val locationsRendered = locations.mkString("\n", "\n", "\n")

    syncDir match {
      case None =>
        if (isClusterConfiguration(locationIO.configuration))
          if (locations.forall(isHdfsLocation)) None.right
          else s"the sync directory must be defined when the configuration is using the cluster and some input/output locations are not hdfs. Got $locationsRendered".left
        else
        if (locations.forall(isLocalLocation)) None.right
        else s"the sync directory must be defined when the configuration is local and some input/output locations are not local. Got $locationsRendered".left

      case Some(h @ HdfsLocation(_)) =>
        if (isClusterConfiguration(locationIO.configuration))
          if (locations.forall(isHdfsLocation)) s"all input/output locations are defined on hdfs. In that case no sync directory should be defined. Got ${h.render}".left
          else ExecutionLocation.fromLocation(h).right
        else
          s"the sync directory can not be defined on the cluster when the configuration is local. Got ${h.render}".left

      case Some(l @ LocalLocation(_)) =>
        if (isClusterConfiguration(locationIO.configuration))
          s"the sync directory can not be defined locally when the configuration is on hdfs. Got ${l.render}".left
        else
        if (locations.forall(isLocalLocation)) s"all input/output locations are defined locally. In that case no sync directory should be defined. Got ${l.render}".left
        else ExecutionLocation.fromLocation(l).right

      case Some(s @ S3Location(_, _)) =>
        s"the sync directory can not be defined on S3. Got ${s.render}".left
    }
  }

  /**
   * Create a synchronized location based on the location of the sync directory if there is one
   */
  def createSynchronizedLocation(syncDir: Option[ExecutionLocation], location: Location): String \/ SynchronizedLocation = {
    (syncDir, location) match {
      // cluster execution
      case (Some(sd), l @ LocalLocation(p)) =>
        sd.fold(path => LocalHdfsSync(p, (DirPath.unsafe(path) </> DirPath.unsafe(new Path(p).toUri.getPath)).path),
                path => LocalNoSync(p)).right

      case (Some(sd), HdfsLocation(p)) =>
        sd.fold(path => HdfsNoSync(p).right,
                path => s"A synchronized location can not be on Hdfs when the execution is local. Got $path".left)

      case (Some(sd), S3Location(b, k)) =>
        sd.fold(path => S3HdfsSync(S3Pattern(b, k),  (DirPath.unsafe(path) </> DirPath.unsafe(k)).path),
                path => S3LocalSync(S3Pattern(b, k), (DirPath.unsafe(path) </> DirPath.unsafe(k)).path)).right

      // not defined: execution is either all local or all Hdfs
      case (None, LocalLocation(p))     => LocalNoSync(p).right
      case (None, HdfsLocation(p) )     => HdfsNoSync(p).right
      case (None, s @ S3Location(_, _)) => s"A synchronized location can not be on S3 when no sync directory is defined. Got ${s.render}".left
    }
  }

  /**
   * Download input files or directories, using distcopy to synchronize files or directories when
   * going from S3 to Hdfs
   *
   * @return the list of all synchronized files
   */
  def synchronizeInputs(inputs: List[SynchronizedLocation], locationIO: LocationIO): RIO[List[Location]] = {
    inputs.traverseU(_.fold(
      hdfs             => RIO.ok(Nil: List[Location]),
      local            => RIO.ok(Nil: List[Location]),
      (local,   hdfs)  => locationIO.syncFiles(LocalLocation(local), HdfsLocation(hdfs)),
      (pattern, local) => locationIO.syncFiles(S3Location(pattern.bucket, pattern.unknown), LocalLocation(local)),
      (pattern, hdfs)  => copyFromS3ToHdfs(S3Location(pattern.bucket, pattern.unknown), HdfsLocation(hdfs), locationIO)
    )).map(_.flatten)
  }

  /**
   * Upload output files or directories, using distcopy to synchronize files or directories when
   * going from Hdfs to S3
   *
   * @return the list of all synchronized files
   */
  def synchronizeOutputs(outputLocations: List[SynchronizedLocation], locationIO: LocationIO): RIO[List[Location]] =
    outputLocations.traverseU(_.fold(
      hdfs             => RIO.ok(Nil: List[Location]),
      local            => RIO.ok(Nil: List[Location]),
      (local,   hdfs)  => locationIO.syncFiles(LocalLocation(local), HdfsLocation(hdfs)),
      (pattern, local) => locationIO.syncFiles(LocalLocation(local), S3Location(pattern.bucket, pattern.unknown)),
      (pattern, hdfs)  => copyFromHdfsToS3(HdfsLocation(hdfs), S3Location(pattern.bucket, pattern.unknown), locationIO)
    )).map(_.flatten)

  /** @return true if this configuration is for a cluster job (not local) */
  def isClusterConfiguration(configuration: Configuration): Boolean =
    FileSystem.getDefaultUri(configuration).getScheme == "hdfs"

  /** @return true if this configuration is for a local job */
  def isLocalConfiguration(configuration: Configuration) =
    !isClusterConfiguration(configuration)

  def isHdfsLocation(location: Location): Boolean =
    location match {
      case HdfsLocation(_)  => true
      case LocalLocation(_) => false
      case S3Location(_,_)  => false
    }

  def isLocalLocation(location: Location): Boolean =
    location match {
      case HdfsLocation(_)  => false
      case LocalLocation(_) => true
      case S3Location(_,_)  => false
    }

  /** @return the list of all copied files */
  def copyFromS3ToHdfs(source: S3Location, target: HdfsLocation, locationIO: LocationIO): RIO[List[Location]] =
    locationIO.isDirectory(source) >>= { isDirectory =>
      if (!isDirectory) locationIO.syncFile(source, target).as(List(source))
      else downloadDirectory(source, target, locationIO)
    }

  /** @return the list of all copied files */
  def copyFromHdfsToS3(source: HdfsLocation, target: S3Location, locationIO: LocationIO): RIO[List[Location]] =
    locationIO.isDirectory(source) >>= { isDirectory =>
      if (!isDirectory) locationIO.syncFile(source, target).as(List(source))
      else uploadDirectory(source, target, locationIO)
    }
}


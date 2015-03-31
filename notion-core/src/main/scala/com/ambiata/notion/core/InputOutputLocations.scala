package com.ambiata.notion.core

import com.ambiata.saws.s3.S3Pattern
import org.apache.hadoop.fs.Path
import ExecutionLocation._

/**
 * Type for execution locations.
 *
 * An execution can only take place locally or on Hdfs
 */
sealed trait ExecutionLocation {
  def fold[T](onHdfs: String => T, onLocal: String => T): T

  /** extract a Path for local and hdfs execution in order to pass it to a map-reduce job */
  def path: Path =
    fold(path => new Path(path), path => new Path(path))

  def render: String =
    fold(identity, identity)
}

object ExecutionLocation {

  def fromLocation(location: Location): Option[ExecutionLocation] =
    location match {
      case HdfsLocation(path)  => Some(HdfsExecutionLocation(path))
      case LocalLocation(path) => Some(LocalExecutionLocation(path))
      case S3Location(_, _)    => None
    }

  def HdfsExecutionLocation(p: String): ExecutionLocation =
    new ExecutionLocation {
      def fold[T](onHdfs: String => T, onLocal: String => T): T =
        onHdfs(p)
    }

  def LocalExecutionLocation(p: String): ExecutionLocation =
    new ExecutionLocation {
      def fold[T](onHdfs: String => T, onLocal: String => T): T =
        onLocal(p)
    }
}

/**
 * A Location for input / output files
 *
 * Some inputs/outputs need to be copied to a second location before being used. We have 5 cases:
 *
 *  - local input/output && local execution => no synchronisation required
 *  - hdfs  input/output && hdfs execution  => no synchronisation required
 *  - local input/output && hdfs execution  => synchronisation required
 *  - s3    input/output && local execution => synchronisation required
 *  - s3    input/output && hdfs execution  => synchronisation required
 */
sealed trait SynchronizedLocation {
  def fold[T](
               onLocalNoSync:   String => T,
               onHdfsNoSync:    String => T,
               onLocalHdfsSync: (String, String) => T,
               onS3LocalSync:   (S3Pattern, String) => T,
               onS3HdfsSync:    (S3Pattern, String) => T
               ): T

  def executionLocation: ExecutionLocation =
    fold(path                                => LocalExecutionLocation(path),
         path                                => HdfsExecutionLocation(path),
         (local: String, hdfs: String)       => HdfsExecutionLocation(hdfs),
         (pattern: S3Pattern, local: String) => LocalExecutionLocation(local),
         (pattern: S3Pattern, hdfs: String)  => HdfsExecutionLocation(hdfs))

  def path: Path = executionLocation.path

}

object SynchronizedLocation {
  def LocalNoSync(local: String): SynchronizedLocation = new SynchronizedLocation {
    def fold[T](onLocalNoSync: String => T, onHdfsNoSync: String => T, onLocalHdfsSync: (String, String) => T,
                onS3LocalSync: (S3Pattern, String) => T, onS3HdfsSync: (S3Pattern, String) => T): T =
      onLocalNoSync(local)
  }

  def HdfsNoSync(hdfs: String): SynchronizedLocation = new SynchronizedLocation {
    def fold[T](onLocalNoSync: String => T, onHdfsNoSync: String => T, onLocalHdfsSync: (String, String) => T,
                onS3LocalSync: (S3Pattern, String) => T, onS3HdfsSync: (S3Pattern, String) => T): T =
      onHdfsNoSync(hdfs)
  }

  def LocalHdfsSync(local: String, hdfs: String): SynchronizedLocation = new SynchronizedLocation {
    def fold[T](onLocalNoSync: String => T, onHdfsNoSync: String => T, onLocalHdfsSync: (String, String) => T,
                onS3LocalSync: (S3Pattern, String) => T, onS3HdfsSync: (S3Pattern, String) => T): T =
      onLocalHdfsSync(local, hdfs)
  }

  def S3LocalSync(pattern: S3Pattern, local: String): SynchronizedLocation = new SynchronizedLocation {
    def fold[T](onLocalNoSync: String => T, onHdfsNoSync: String => T, onLocalHdfsSync: (String, String) => T,
                onS3LocalSync: (S3Pattern, String) => T, onS3HdfsSync: (S3Pattern, String) => T): T =
      onS3LocalSync(pattern, local)
  }

  def S3HdfsSync(pattern: S3Pattern, hdfs: String): SynchronizedLocation = new SynchronizedLocation {
    def fold[T](onLocalNoSync: String => T, onHdfsNoSync: String => T, onLocalHdfsSync: (String, String) => T,
                onS3LocalSync: (S3Pattern, String) => T, onS3HdfsSync: (S3Pattern, String) => T): T =
      onS3HdfsSync(pattern, hdfs)
  }
}

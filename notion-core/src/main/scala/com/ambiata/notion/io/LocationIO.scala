package com.ambiata.notion
package io

import java.io.File
import com.ambiata.saws.core.{S3Action, Clients}
import core._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.data.Lists
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.{S3Prefix, S3Address, S3Pattern}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._, effect.IO, effect.Effect._

/**
 * This module provides file system-like functions on HDFS, S3 and locally
 */
case class LocationIO(configuration: Configuration, @transient s3Client: AmazonS3Client) {

  /** @return the (recursive) list of all the locations contained in `location` */
  def list(location: Location): RIO[List[Location]] = location match {
    case l @ LocalLocation(path) =>
      Directories.list(l.dirPath).map(_.map(f => LocalLocation(f.path)))

    case s @ S3Location(bucket, key) =>
      S3Pattern(bucket, key).listKeys.executeT(s3Client).map(_.map(k => S3Location(bucket,  k)))

    case h @ HdfsLocation(path) =>
      Hdfs.globFilesRecursively(new Path(path)).run(configuration).map(_.map(p => HdfsLocation(p.toString)))
  }

  /** @return true if the location exists */
  def exists(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)            =>
      Files.exists(FilePath.unsafe(path)).flatMap(e => if (e) ResultT.ok[IO, Boolean](e) else Directories.exists(l.dirPath))

    case s @ S3Location(bucket, key) => S3Pattern(bucket, key).exists.executeT(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.exists(new Path(path)).run(configuration)
  }

  /** @return true if the location can contain other locations */
  def isDirectory(location: Location): RIO[Boolean] = location match {
    case l @ LocalLocation(path)     => ResultT.safe[IO, Boolean](new File(path).isDirectory)
    case s @ S3Location(bucket, key) => S3Pattern(bucket, key + "/").determine.map(_.exists(_.isRight)).executeT(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.isDirectory(new Path(path)).run(configuration)
  }

  /** @return the lines of the file present at `location` if there is one */
  def readLines(location: Location): RIO[List[String]] = location match {
    case l @ LocalLocation(path)     => Files.readLines(l.filePath).map(_.toList)
    case s @ S3Location(bucket, key) =>
      S3Pattern(bucket, key).determine.flatMap { prefixOrAddress =>
        val asAddress = prefixOrAddress.flatMap(_.swap.toOption)
        val lines = asAddress.map(_.getLines).getOrElse(S3Action.fail(s"There is no file at ${location.render}"))
        lines
      }.executeT(s3Client)

    case h @ HdfsLocation(path)      =>
      Hdfs.isDirectory(new Path(path)).flatMap { isDirectory =>
        if (isDirectory)
          Hdfs.globFilesRecursively(new Path(path)).filterHidden
            .flatMap(_.traverseU(Hdfs.readLines)).map(_.toList.flatten)
        else
          Hdfs.readLines(new Path(path)).map(_.toList)
      }.run(configuration)
  }

  def readUnsafe(location: Location)(f: java.io.InputStream => RIO[Unit]): RIO[Unit] =
    location match {
      case l @ LocalLocation(_) =>
        ResultT.using(l.filePath.toInputStream)(f)
      case S3Location(bucket, key) =>
        ResultT.using(S3Address(bucket, key).getObject.map(_.getObjectContent).executeT(s3Client))(f)
      case HdfsLocation(path) =>
        Hdfs.readWith(new Path(path), f).run(configuration)
    }

  def streamLinesUTF8[A](location: Location, empty: => A)(f: (String, A) => A): RIO[A] =
    ResultT.io(empty).flatMap { s =>
      var state = s
      readUnsafe(location) { in => ResultT.io {
        val reader = new java.io.BufferedReader(new java.io.InputStreamReader(in, "UTF-8"))
        var line = reader.readLine
        while (line != null) {
          state = f(line, state)
          line = reader.readLine
        }
      }}.as(state)
    }

  def writeUtf8Lines(location: Location, lines: List[String]): RIO[Unit] =
    writeUtf8(location, Lists.prepareForFile(lines))

  def writeUtf8(location: Location, string: String): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Files.write(l.filePath, string)
    case s @ S3Location(bucket, key) => S3Address(bucket, key).put(string).executeT(s3Client).void
    case h @ HdfsLocation(path)      => Hdfs.writeWith(new Path(path), out => Streams.write(out, string)).run(configuration)
  }

  def deleteAll(location: Location): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Directories.delete(l.dirPath).void
    case s @ S3Location(bucket, key) => S3Prefix(bucket, key).delete.executeT(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.deleteAll(new Path(path)).run(configuration)
  }

  def delete(location: Location): RIO[Unit] = location match {
    case l @ LocalLocation(path)     => Files.delete(l.filePath)
    case s @ S3Location(bucket, key) => S3Address(bucket, key).delete.executeT(s3Client)
    case h @ HdfsLocation(path)      => Hdfs.delete(new Path(path)).run(configuration)
  }

}

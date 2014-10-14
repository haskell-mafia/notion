package com.ambiata.notion.distcopy

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit


class S3Split(var workload: Workload) extends InputSplit with Writable {
  def this() = this(null)

  def getLength: Long = workload.indexes.size

  def getLocations: Array[String] = Array.empty[String]

  def write(out: DataOutput) = {
    out.writeInt(workload.indexes.size)
    workload.indexes.foreach(z => out.writeInt(z))
  }

  def readFields(in: DataInput) = {
    val size = in.readInt
    workload = Workload((1 to size).map(_ => in.readInt).toVector)
  }
}

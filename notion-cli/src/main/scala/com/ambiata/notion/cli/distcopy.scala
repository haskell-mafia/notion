package com.ambiata.notion.cli

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core.{HdfsLocation, S3Location, Location}
import com.ambiata.notion.distcopy._
import com.ambiata.saws.core.Clients
import com.ambiata.saws.s3._
import MemoryConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._, effect._

/**
 * Command line application to use distcopy
 */
object distcopy extends App {
  val parser = new scopt.OptionParser[InputMappings]("notion - distcopy") {

    head(s"""
        |notion - distcopy commands.
        |""".stripMargin)

    help("help") text "shows this usage text"
    version("version") text "shows the version text"
    note("===========================================================")
    message
    note("===========================================================")

    def message = {
      opt[String]('i', "input").text("""Input file. Mappings seperated by line and Source/Destination seperated by comma
                                       |i.e.
                                       |s3://source,hdfs://output
                                       |hdfs://source,s3://output
                                       |""".stripMargin).
        action((a, m) => m.copy(input = a))

      opt[Int]('m', "mappers").text("Number of mappers, 20 by default").
        action((a, m) => m.copy(mappers = a))
      opt[Int]('r', "retry").text("Retry count on mapper").
        action((a, m) => m.copy(retry = a))
      opt[Int]('p', "partsize").text("Part size, 10mb by default").
        action((a, m) => m.copy(partSize = a))
      opt[Int]('l', "readlimit").text("Readlimit, 10mb by default").
        action((a, m) => m.copy(readLimit = a))
      opt[Int]('t', "uploadthreshold").text("Multipart upload threshold, 100mb by default").
        action((a, m) => m.copy(multipartUploadThreshold = a))

    }

  }

  def mapping(source: String, destination: String): Option[Mapping] =
    Location.fromUri(source) match {
      case \/-(S3Location(b, k)) =>
        Location.fromUri(destination) match {
          case \/-(HdfsLocation(p)) =>
            DownloadMapping(S3Address(b, k), new Path(p)).some
          case _ =>
            none
        }
      case \/-(HdfsLocation(p))  =>
        Location.fromUri(destination) match {
          case \/-(S3Location(b, k)) =>
            UploadMapping(new Path(p), S3Address(b, k)).some
          case _ =>
            none
        }
      case _ =>
        none
    }

  parser.parse(args, InputMappings("", 20, 3, 10, 10, 100)) match {
    case None =>
      exit(1)

    case Some(m) =>
      val conf = new Configuration()
      val client = Clients.s3
      val distcopy: ResultTIO[Unit] = if (m.input.nonEmpty) {
        for {
          lines <- Files.readLines(FilePath.unsafe(m.input))
          pairs <- lines.traverse(s => s.split(",").toList match {
            case f :: t :: Nil =>
              (f, t).pure[ResultTIO]
            case _ =>
              ResultT.failIO[(String, String)]("Can not parse input file")
          })

          mappings = pairs.map((mapping _).tupled).map(_ match {
            case Some(s) =>
              s
            case None =>
              exit(1)
          })
          r <- DistCopyJob.run(
              Mappings(mappings)
            , DistCopyConfiguration(conf, client, m.mappers, m.retry, m.partSize.mb, m.readLimit.mb , m.multipartUploadThreshold.mb)
          )
        } yield r
      } else
        ResultT.fail[IO, Unit]("No inputs")

      distcopy.run.unsafePerformIO() match {
        case Error(e) =>
          println(Result.asString(e))
          exit(1)
        case Ok(_) =>
          println("distcopy successful")
          exit(0)
      }
  }

  private def exit(code: Int) = {
    sys.exit(code)
  }
}

case class InputMappings(input: String,
                         mappers: Int,
                         retry: Int,
                         partSize: Int,
                         readLimit: Int,
                         multipartUploadThreshold: Int)

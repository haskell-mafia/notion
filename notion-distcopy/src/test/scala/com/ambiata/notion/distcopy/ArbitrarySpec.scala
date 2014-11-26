package com.ambiata.notion.distcopy

import org.specs2._
import Arbitraries._

import org.apache.hadoop.fs.Path

class ArbitrarySpec extends Specification with ScalaCheck { def is = s2"""

Distcopy Arbitraries
====================

 Path
 ----
  Should be a valid absolute hdfs location
   ${ prop((x: Path) => x.toString.startsWith("/") must_== true) }

  Non empty
   ${ prop((x: Path) => x.toString.nonEmpty ==== true) }

  Valid characters
   ${ prop((x: Path) => x.toString.forall(c => c.equals('/') || c.isLetter || c.isDigit)  ==== true) }

 Mapping
 -------
  Non empty
   ${ prop((m: DownloadMapping) => m.from.render.nonEmpty && m.to.toString.nonEmpty ==== true) }
   ${ prop((m: UploadMapping) => m.from.toString.nonEmpty && m.to.render.nonEmpty ==== true) }

  Valid locations
   ${ prop((m: DownloadMapping) => m.from.render.startsWith("/") ==== false) }
   ${ prop((m: DownloadMapping) => m.to.toString.startsWith("/") ==== true) }
   ${ prop((m: UploadMapping) => m.from.toString.startsWith("/") ==== true) }
   ${ prop((m: UploadMapping) => m.to.render.startsWith("/") ==== false) }

 Mappings
 --------
  Non empty
   ${ prop((m: Mappings) => m.mappings.nonEmpty ==== true) }


"""
}

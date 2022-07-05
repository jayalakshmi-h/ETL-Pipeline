package utils

import java.io.InputStream

import model.FileFormat

import scala.util.{Failure, Success, Try}

trait FormatFileReader {



  def readFormat(fileName: String) : Vector[FileFormat] = {
    val selectStatsTry = Try {
      val stream: InputStream = getClass.getResourceAsStream(fileName)
      val lines = scala.io.Source.fromInputStream(stream).getLines.toVector
      lines.flatMap { line =>
        strOrNone(line.split('#')(0)).flatMap { statLine =>
          statLine.split(",").toList match {
            case colName :: start :: end :: dType ::Nil => Some(FileFormat(colName, start.toInt, end.toInt, dType))
            case _ => None
          }
        }
      }
    }
    selectStatsTry match {
      case Success(stats) =>
        stats
      case Failure(_) =>
        Vector.empty
    }

  }

  def strOrNone(strRaw: String): Option[String] = {
    val trimmedStr = strRaw.trim
    if (trimmedStr.isEmpty || trimmedStr.dropWhile(_ == '-').isEmpty) None else Some(trimmedStr)

  }




}




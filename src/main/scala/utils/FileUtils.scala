package utils

import java.io.File

object FileUtils {
  def getListOfFiles(basePath : String) = {
    val file = new File(basePath)
    file.listFiles.filter(_.isFile).map(_.getPath).toList
  }

}

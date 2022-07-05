import java.sql.SQLException

import model.{Data, FileFormat, Header, IgraData}
import utils.{ConnectionPool, FileUtils, FormatFileReader}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * 1. Get the zip files from the base path
 *  2. Get an inputstream for the textfiles
 *  3. Get an iterator to read the file line by line
 *  4. Parse each line using the appropriate formatter (header/data)
 *  5. Append the data to the buffer
 *  6. Flush it to DB once it reaches the threshold
 *
 *  Results extract from DB
 * postgres=# select count(headerId) from data;
 * count
 * ----------
 * 23796788
 * (1 row)
 *
 * postgres=# select count(headerId) from header;
 * count
 * --------
 * 290275
 * (1 row)
 *
 *
 */

object pushDataToPostgres extends App with FormatFileReader {

  val basePath = "/Users/jayalakshmi/Downloads/input"

  lazy implicit private val context: ExecutionContext = ExecutionContext.global

  classOf[org.postgresql.Driver]
  val con_str = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=password"
  private val buffer = mutable.ListBuffer.empty[IgraData]

  var hdrId = 0
  val bufferThreshold = 1000

   val res = Future {

    val headerFormat = readFormat("header_format.txt")
    val dataFormat =  readFormat("data_format.txt")
    val zipFilePath = FileUtils.getListOfFiles(basePath)
    zipFilePath.foreach(p  => {
      val rootzip = new java.util.zip.ZipFile(p)
      val entries = rootzip.entries.asScala
        entries.foreach { e =>
          val is = rootzip.getInputStream(e)
        val lines : Iterator[String] = scala.io.Source.fromInputStream(is).getLines()
            lines.foreach(line => {
                val igraData : IgraData = if(line.startsWith("#")) {
                   hdrId = hdrId + 1
                   createCaseClass[Header](parse(line, headerFormat,hdrId))
                 }
                 else  createCaseClass[Data](parse(line, dataFormat,hdrId))
                 buffer.append(igraData)
                 mayBeFlushBuffer()
               })
        }
    }
    )
  }

  Await.ready(res, Duration.Inf)

  private def mayBeFlushBuffer() : Unit = {
    if (buffer.size > bufferThreshold) flushBuffer()
  }

  lazy implicit private val pool: ConnectionPool = ConnectionPool.postgresConnectionPool

 private def flushBuffer() = {

     if(buffer.nonEmpty)  {
       val conn = pool.getConnection
       val headerPs = conn.prepareStatement("INSERT INTO header(HEADREC,ID,YEAR,MONTH,DAY,HOUR,RELTIME,NUMLEV,P_SRC,NP_SRC,LAT,LON,headerId) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
       val dataPs = conn.prepareStatement("INSERT INTO data(LVLTYP1,LVLTYP2,ETIME,PRESS,PFLAG,GPH,ZFLAG,TEMP,TFLAG,RH,DPDP,WDIR,WSPD,headerId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)")
           try {

             buffer.foreach {
               case message: Header =>
                 {
                   headerPs.setString(1, message.HEADREC.orNull)
                   headerPs.setString(2, message.ID.orNull)
                   headerPs.setString(3, message.YEAR.orNull)
                   headerPs.setString(4, message.MONTH.orNull)
                   headerPs.setString(5, message.DAY.orNull)
                   headerPs.setInt(6, message.HOUR.getOrElse(None).asInstanceOf[Int])
                   headerPs.setInt(7, message.RELTIME.getOrElse(None).asInstanceOf[Int])
                   headerPs.setInt(8, message.NUMLEV.getOrElse(None).asInstanceOf[Int])
                   headerPs.setString(9, message.P_SRC.orNull)
                   headerPs.setString(10, message.NP_SRC.orNull)
                   headerPs.setInt(11, message.LAT.getOrElse(None).asInstanceOf[Int])
                   headerPs.setInt(12, message.LON.getOrElse(None).asInstanceOf[Int])
                   headerPs.setLong(13, message.headerId)
                   headerPs.addBatch()
                 }
               case message: Data => {
                 dataPs.setInt(1, message.LVLTYP1.getOrElse(None).asInstanceOf[Int])
                 dataPs.setInt(2, message.LVLTYP2.getOrElse(None).asInstanceOf[Int])
                 dataPs.setInt(3, message.ETIME.getOrElse(None).asInstanceOf[Int])
                 dataPs.setInt(4, message.PRESS.getOrElse(None).asInstanceOf[Int])
                 dataPs.setString(5, message.PFLAG.orNull)
                 dataPs.setInt(6, message.GPH.getOrElse(None).asInstanceOf[Int])
                 dataPs.setString(7, message.ZFLAG.orNull)
                 dataPs.setInt(8, message.TEMP.getOrElse(None).asInstanceOf[Int])
                 dataPs.setString(9, message.TFLAG.orNull)
                 dataPs.setInt(10, message.RH.getOrElse(None).asInstanceOf[Int])
                 dataPs.setInt(11, message.DPDP.getOrElse(None).asInstanceOf[Int])
                 dataPs.setInt(12, message.WDIR.getOrElse(None).asInstanceOf[Int])
                 dataPs.setInt(13, message.WSPD.getOrElse(None).asInstanceOf[Int])
                 dataPs.setLong(14, message.headerId)
                 dataPs.addBatch()
               }
             }
             headerPs.executeBatch()
             dataPs.executeBatch()
             buffer.clear()
           } catch {
             case exception: SQLException =>
               println(s"Encountered SQL exception: $exception", exception)
               throw exception
           }
           finally {
             headerPs.close()
             dataPs.close()
             conn.close()
           }
       }
     }



   def parse(s : String, fileFormat: Vector[FileFormat], hdrId : Long)  = {
        val map = fileFormat.foldLeft(Map.empty[String, Object])((acc, rec) => {
           acc ++ Map(rec.colName -> cast(s.slice(rec.start - 1, rec.end).trim, rec.dType))
        })
      map ++ Map("headerId" -> hdrId)
   }

   def cast(s : String, dType : String) : Option[Any] = {
      dType match {
         case "int" => Try(s.toInt).toOption
         case _ => Some(s)
      }
   }

   def createCaseClass[T](vals : Map[String, Any])(implicit cmf : ClassTag[T]) = {
      val ctor = cmf.runtimeClass.getConstructors.head
      val args = cmf.runtimeClass.getDeclaredFields.map( f => vals(f.getName) )
      ctor.newInstance(args : _*).asInstanceOf[T]
   }
}

package utils

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

trait ConnectionPool {

  val scheme: String
  val database: String
  val host: String
  val port: Int
  protected val userName: String
  protected val password: String

  val poolSize: Int
  val driverClass: Class[_]
  private val pool: scala.collection.mutable.Queue[Connection] = scala.collection.mutable.Queue[Connection]()

  /**
   * For getting connection object from connection pool.
   * @return connection object.
   */
  def getConnection: Connection = {
    if (pool.size >= poolSize) {
      throw new SQLException("No connection left in pool")
    } else if (pool.isEmpty) {
     createConnection
    } else {
      pool.dequeue()
    }
  }

  private def createConnection = {
    try {
      DriverManager.getConnection(s"jdbc:$scheme://$host:$port/$database", userName, password)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        throw e
      case e: SQLException =>
        e.printStackTrace()
        throw e
    }

  }

  /**
   * For returning connection object to the connection pool.
   * @param connection
   */
  def returnConnection(connection: Connection): Unit = {
    pool.enqueue(connection)
  }
}



object ConnectionPool {

  def postgresConnectionPool: ConnectionPool = new ConnectionPool {
    override val scheme = "postgresql"
    override val host = "localhost"
    override val port = 5432
    override val database = "postgres"
    override protected val userName = "postgres"
    override protected val password = "password"

    override val poolSize = 5
    override val driverClass = classOf[org.postgresql.Driver]
  }

}

/**
 * PreparedStatement helper functions for a provided datatype.
 *
 * @tparam Metadata
 */
trait Prepare[Metadata] {

  def statement(connection: Connection, databaseSchema: String, tableName: String): PreparedStatement

  def addBatch(preparedStatement: PreparedStatement, metadata: Metadata): Unit

}


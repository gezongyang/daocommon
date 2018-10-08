package com.anallslsoftax.pool

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.anallslsoftax.conf.ConfigurationManager
import com.anallslsoftax.constant.Constants
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}



trait QueryCallback {
  def process(rs: ResultSet)
}

/**
  * Created by gzy
  *
  * MySQL 客户端代理对象
  * @param jdbc
  * @param user
  * @param passwd
  * @param client
  */
case class MySqlProxy(jdbc: String, user: String, passwd: String, client: Option[Connection] = None) {

  //mysqlClient
  private val mysqlClient = client.getOrElse {
    DriverManager.getConnection(jdbc, user, passwd)
  }


  /**
    * 执行增删改操作
    * @param sql
    * @param params
    * @return 影响的行数
    */
  def executeUpdate(sql: String, params: Array[Any]) : Int = {

    var rtn = 0;
    var ps :PreparedStatement = null;


    try {
      mysqlClient.setAutoCommit(false)

      ps = mysqlClient.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          ps.setObject(i +1, params(i))
        }
      }
      rtn = ps.executeUpdate()
      mysqlClient.commit()

    } catch {
      case  e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 执行查询
    * @param sql
    * @param params
    * @param queryCallback
    */
  def executeQuery(sql: String, params: Array[Any], queryCallback: QueryCallback): Unit = {

    var ps: PreparedStatement = null
    var resultSet: ResultSet = null;

    try {
      ps = mysqlClient.prepareStatement(sql)

      if (params != null && params.length >0) {
        for (i <- 0 until params.length) {
          ps.setObject(i + 1, params(i))
        }
      }

      resultSet = ps.executeQuery()

      queryCallback.process(resultSet)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 批量执行SQL语句
    *
    * @param sql
    * @param paramsList
    * @return
    */
  def executeBatch(sql: String, paramsList: Array[Array[Any]]): Array[Int] = {

    var rnt: Array[Int] = null;
    var ps : PreparedStatement = null;

    try {
      mysqlClient.setAutoCommit(false)
      ps = mysqlClient.prepareStatement(sql)

      if (paramsList != null && paramsList.length > 0) {

        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            ps.setObject(i +1, params(i))
          }

          ps.addBatch()
        }
      }

      rnt = ps.executeBatch()
      mysqlClient.commit()

    } catch {
      case e: Exception => e.printStackTrace
    }

    rnt
  }



  //关闭客户端
  def shutdown(): Unit = mysqlClient.close()

}



/**
  * 通过继承基础连接池并提供一个池化对象的类型来得到一个连接池创建的工厂
  * @param jdbc
  * @param user
  * @param passwd
  * @param client
  */
class MysqlClientFactory (jdbc: String, user: String, passwd: String, client: Option[Connection] = None)
           extends BasePooledObjectFactory[MySqlProxy] with Serializable{

   //连接池创建对象
    override def create(): MySqlProxy = MySqlProxy(jdbc, user, passwd, client)

    //连接池包装对象
    override def wrap(obj : MySqlProxy): PooledObject[MySqlProxy] = new DefaultPooledObject[MySqlProxy](obj)


    override def destroyObject(p: PooledObject[MySqlProxy]): Unit = {

    //关闭客户端
    p.getObject.shutdown()
    super.destroyObject(p)

  }
  }


/**
  * 创建连接池工具类
  */
object MysqlPool {

  //加载JDBC驱动，只需要一次
  Class.forName("com.mysql.jdbc.Driver")

  private var genericObjectPool:GenericObjectPool[MySqlProxy] = null

  //获取对象池
  def apply(): GenericObjectPool[MySqlProxy] = {
     if (this.genericObjectPool == null) {
       this.synchronized {

         val jdbcUrl = ConfigurationManager.config.getString(Constants.JDBC_URL)
         val jdbcUser = ConfigurationManager.config.getString(Constants.JDBC_USER)
         val jdbcPasswd = ConfigurationManager.config.getString(Constants.JDBC_PASSWD)
         val size = ConfigurationManager.config.getInt(Constants.JDBC_DATASOURCE_SIZE)

         val pooledFactory = new MysqlClientFactory(jdbcUrl, jdbcUser, jdbcPasswd)
         val configOfPool = {
           val config = new GenericObjectPoolConfig
           config.setMaxTotal(size)
           config.setMaxIdle(size)
           config
         }

         this.genericObjectPool = new GenericObjectPool[MySqlProxy](pooledFactory, configOfPool)
       }
     }

     genericObjectPool
  }
}

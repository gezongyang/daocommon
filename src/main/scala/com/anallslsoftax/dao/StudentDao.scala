package com.anallslsoftax.dao

import com.anallslsoftax.model.Student
import com.anallslsoftax.pool.MysqlPool

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gzy
  */
object StudentDao {

  def insertBach(students :Array[Student]) {

    //获取连接池
    val mysqlPool = MysqlPool()

    val client = mysqlPool.borrowObject()

    val insertSQL = "insert into student(id,name,gender,age) values (?,?,?,?)"

    val insertParamsList : ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (student <- students) {
      insertParamsList += Array[Any](student.id,student.name,student.gender,student.age)
    }


    client.executeBatch(insertSQL, insertParamsList.toArray)

    //使用完成后将对象返回给对象池
    mysqlPool.returnObject(client)
  }
}

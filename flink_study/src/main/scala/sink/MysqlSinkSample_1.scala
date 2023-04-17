package sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyParallelSource
import transform.Brand_Event

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Random

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 自定义sink写入Mysql
 *
 * @author 须贺
 * @version 2023/4/16 
 */
object MysqlSinkSample_1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.addSource(new MyParallelSource).setParallelism(2)
    val res=data.map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })
    res.print()

    //存储到mysql
    res.addSink(new MysqlSink)


    env.execute()
  }
}

case class Brand_Info(
                       brand_id: String,
                       brand_name: String,
                       telephone: String,
                       brand_status: String
                     )

class MysqlSink extends RichSinkFunction[Brand_Info] {
  // 连接 MySQL 数据库的相关配置
  val jdbcUrl = "jdbc:mysql://bigdata1:3306/test?useSSL=false&characterEncoding=utf8"
  val username = "root"
  val password = "123456"
  var connection: Connection = _
  var ps: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 加载 JDBC 驱动
    Class.forName("com.mysql.jdbc.Driver") //这步可省略
    connection = DriverManager.getConnection(jdbcUrl, username, password)
    // replace into 根据索引，会尝试插入数据，如果插入的行存在，则会先删除该行，然后再插入新的数据，
    // 如果没有设置索引，整个列集合进行比较
    ps = connection.prepareStatement(
      """
        |replace into brand_info(
        |brand_id,
        |brand_name,
        |telephone,
        |brand_status
        |)
        |values(?,?,?,?)
        |""".stripMargin)
  }

  override def invoke(value: Brand_Info, context: SinkFunction.Context): Unit = {
    ps.setString(1,value.brand_id)
    ps.setString(2,value.brand_name)
    ps.setString(3,value.telephone)
    ps.setString(4,value.brand_status)
    ps.execute()
  }

  override def close(): Unit = {
    if(ps!=null){
      ps.close()
    }
    if(connection!=null){
      connection.close();
    }
  }
}



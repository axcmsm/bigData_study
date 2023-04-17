package table

import com.alibaba.fastjson.JSON
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Schema, TableDescriptor}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.TableEnvironment

import java.sql.Timestamp

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * FlinkSQL 案例
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object FlinkSQLBase {

  def main(args: Array[String]): Unit = {
    //    val settings=EnvironmentSettings.newInstance().inStreamingMode().build()//.inBatchMode()
    //    val tEnv = TableEnvironment.create(settings)

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建 Table 环境
    val tableEnv = StreamTableEnvironment.create(env)

    //kafka的数据格式：
    //{"guid":1,"sessionId":"s01","eventId":"e01","eventTime":1000}
    //{"guid":1,"sessionId":"s01","eventId":"e02","eventTime":2000}
    //{"guid":1,"sessionId":"s01","eventId":"e03","eventTime":3000}
    //{"guid":2,"sessionId":"s02","eventId":"e02","eventTime":2000}
    //{"guid":2,"sessionId":"s02","eventId":"e02","eventTime":3000}
    //{"guid":2,"sessionId":"s02","eventId":"e01","eventTime":4000}
    //{"guid":2,"sessionId":"s02","eventId":"e03","eventTime":5000}
    //需求：每个用户每次会话中各类行为的发生次数，并将结果输出到控制台

    //混搭
    val source = env
      .socketTextStream("master", 7777)
      .map(JSON.parseObject(_, classOf[Log_Event]))

    //创建表
    tableEnv.createTemporaryView("log_event", source) //表
    //createTable 永久表：schema 可记录在外部持久化的元数据管理器中（比如 hive 的 metastore）
    //createTemporaryView 临时表 ： schema 只维护在所属 flink session 运行时内存中；
    // 无论是表还是视图都是一个table对象
    //val table = tableEnv.fromDataStream(source)//表对象


    //需求：每个用户每次会话中各类行为的发生次数，并将结果输出到控制台
    tableEnv.executeSql(
      """
        |select
        |guid,sessionId,eventId,count(1)
        |from log_event
        |group by guid,sessionId,eventId
        |""".stripMargin).print()

    env.execute("Flink SQL Example")
  }
}

//{"guid":1,"sessionId":"s01","eventId":"e01","eventTime":1000}
case class Log_Event(
                      guid: Int,
                      sessionId: String,
                      eventId: String,
                      eventTime: Long
                    )

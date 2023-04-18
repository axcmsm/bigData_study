package table.tableConnector

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import table.Log_Event

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object HbaseConnectorSample {
  def main(args: Array[String]): Unit = {
    // TODO: hbase connect
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(env)

    //我们有一个 Kafka 主题，其中包含着用户的基本信息（如：用户ID、用户名、年龄等）
    //{"id":92332,"name":"实习生","age":259}
    //{"id":922322,"name":"阿斯顿","age":209}
    //{"id":92232,"name":"弗格森","age":409}
    // source
     tableEnvironment.executeSql(
       """
         |CREATE TEMPORARY TABLE kafka_table (
         |    id BIGINT,
         |    name STRING,
         |    age INT
         |) WITH (
         |   'connector'='kafka',
         |    'topic'='user_topic',
         |    'properties.bootstrap.servers'='bigdata1:9092,bigdata2:9092,bigdata3:9092',
         |    'properties.group.id'='group-test',
         |    'format'='json',
         |    -- 'scan.startup.mode'='earliest-offset'
         |    'scan.startup.mode'='latest-offset'
         |)
         |""".stripMargin)

    //sink
    tableEnvironment.executeSql(
      """
        |CREATE TEMPORARY TABLE hbase_table (
        |    rowkey STRING,
        |    info ROW<id BIGINT,name STRING,age INT>
        |) WITH (
        |'connector' = 'hbase-2.2',
        |'table-name' = 'user_table',
        |'zookeeper.quorum' = 'bigdata1:2181,bigdata2:2181,bigdata3:2181'
        |)
        |""".stripMargin)

    //输出
   tableEnvironment.executeSql(
      """
        |insert into hbase_table
        |SELECT
        |  CONCAT(cast(id as string),'_',DATE_FORMAT(NOW(), 'yyyyMMddHHmmss')) AS rowkey,
        | row(id, name, age) as info
        |FROM kafka_table
        |""".stripMargin)


    //    env.execute()
  }
}

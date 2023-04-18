package table.tableConnector

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object ClickHouseConnectorSample {
  def main(args: Array[String]): Unit = {
    // TODO: clickhouse connect
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(env)
    tableEnvironment.executeSql(
      """
        |CREATE TABLE jdbc_table (
        | `brand_id` int ,
        |  `brand_name` string,
        |  `telephone` string,
        |  `brand_web` string,
        |  `brand_logo` string,
        |  `brand_desc` string,
        |  `brand_status` int ,
        |  `brand_order` int ,
        |  `modified_time` timestamp
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:clickhouse://bigdata1:8123/default',
        |    'table-name' = 'brand_info',
        |    'driver'='ru.yandex.clickhouse.ClickHouseDriver',
        |    'username' = 'default',
        |    'password' = '123456'
        |)
        |""".stripMargin)
    tableEnvironment.executeSql("select * from jdbc_table").print()


  }
}

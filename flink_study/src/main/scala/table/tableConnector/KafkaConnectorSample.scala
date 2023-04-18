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
object KafkaConnectorSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(env)

    // TODO: kafka connect
    //1,204,萨蒂
    //2,203,无慢
    //1|223|萨蒂
    //2|224|巫启贤
    tableEnvironment.executeSql(
      """
        |CREATE TABLE KafkaTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `behavior` STRING,
        |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        |  `pt` as proctime()
        |) WITH (
        |   'connector'='kafka',
        |   'topic'='test5',
        |   'properties.bootstrap.servers'='bigdata1:9092,bigdata2:9092,bigdata3:9092',
        |   'properties.group.id'='group-test',
        |   'format'='csv',
        |   'csv.field-delimiter'='|',
        |   -- 'scan.startup.mode'='earliest-offset',
        |   'scan.startup.mode'='latest-offset'
        |)
        |""".stripMargin)
    tableEnvironment.executeSql("select * from KafkaTable").print()



    // TODO: Upset-kafka connect
    // 对于 -U/+U/+I 记录， 都以正常的 append 消息写入 kafka；
    // 对于-D 记录，则写入一个 null 到 kafka 来表示 delete 操作；
    tableEnvironment.executeSql(
      """
        |CREATE TABLE KafkaTable (
        |  `user_id` BIGINT,
        |  `item_id` BIGINT,
        |  `behavior` STRING,
        |  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        |  `pt` as proctime()
        |) WITH (
        |   'connector'='upsert-kafka',
        |   'topic'='test5',
        |   'properties.bootstrap.servers'='bigdata1:9092,bigdata2:9092,bigdata3:9092',
        |   'properties.group.id'='group-test',
        |   'format'='csv',
        |   'csv.field-delimiter'='|',
        |   -- 'scan.startup.mode'='earliest-offset',
        |   'scan.startup.mode'='latest-offset'
        |)
        |""".stripMargin)
    tableEnvironment.executeSql("select * from KafkaTable").print()



    env.execute()
  }
}

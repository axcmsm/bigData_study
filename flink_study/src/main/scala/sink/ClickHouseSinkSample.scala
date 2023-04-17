package sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyParallelSource

import java.sql.PreparedStatement

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 写入数据到Clickhouse
 *
 * @author 须贺
 * @version 2023/4/16 
 */
object ClickHouseSinkSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)
    val res=data.map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })
    res.print()

    //写入数据到clickhouse
    //因为clickhouse 支持jdbc操作，可以直接使用jdbc进行连接
    //jdbc的方式有俩种：一种是使用自定义sink 的方式实现，一种是使用 connector jdbc的方式进行实现
    val mysqlSink = JdbcSink.sink(
      """
        |insert into brand_info(
        |brand_id,
        |brand_name,
        |telephone,
        |brand_status
        |)
        |values(?,?,?,?)
        |""".stripMargin,
      new JdbcStatementBuilder[Brand_Info] {
        override def accept(t: PreparedStatement, u: Brand_Info): Unit = {
          t.setString(1, u.brand_id)
          t.setString(2, u.brand_name)
          t.setString(3, u.telephone)
          t.setString(4, u.brand_status)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(5)
        .withMaxRetries(3)
        .withBatchIntervalMs(5000)
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:clickhouse://slave2:8123/default?useSSL=false&characterEncoding=utf8")
        .withUsername("default")
        .withPassword("passwd")
        .build()
    )
    res.addSink(mysqlSink)

    env.execute()
  }
}
//创建表 使用了默认的引擎
/**
create table brand_info(
brand_id String,
brand_name String,
telephone String,
brand_status String
) engine =MergeTree
order by brand_id;

 */

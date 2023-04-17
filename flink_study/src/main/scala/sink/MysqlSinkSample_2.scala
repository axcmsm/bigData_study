package sink

import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.function.SerializableSupplier
import source.MyParallelSource
import transform.Brand_Event

import java.sql.{Connection, DriverManager, PreparedStatement}
import javax.sql.XADataSource
import scala.util.Random

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 使用JDBC连接器进行写入
 * @author 须贺
 * @version 2023/4/16 
 */
object MysqlSinkSample_2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("file:///E:/ckpt")
    val data = env.addSource(new MyParallelSource).setParallelism(1)
    val res=data.map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })
    res.print()


    //JDBC Sink
    //该 sink 的底层并没有去开启 mysql 的事务，所以并不能真正保证 端到端的 精确一次
    val mysqlSink = JdbcSink.sink(
      """
        |replace into brand_info(
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
        .withBatchSize(5) //量写入时的缓存大小，默认是：缓存5000条后，立即写入数据库
        .withMaxRetries(3) //默认是3，最大重试次数
        .withBatchIntervalMs(5000) //批量写入时的时间间隔，默认值为 0 (毫秒)，即不启用批量写入功能
        .build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://bigdata1:3306/test?useSSL=false&characterEncoding=utf8")
        .withUsername("root")
        .withPassword("123456")
        //.withDriverName("com.mysql.jdbc.Driver")
        .build()
    )
    res.addSink(mysqlSink)


    // 可以保证精确一次的Sink，exactlyOnceSink
    /*val MysqlExactlyOnceSink: SinkFunction[Brand_Info] = JdbcSink.exactlyOnceSink(
      """
        |replace into brand_info(
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
        .withBatchSize(5) //量写入时的缓存大小
        .withMaxRetries(3) //最大重试次数
        .withBatchIntervalMs(5000) //批量写入时的时间间隔
        .build(),
      JdbcExactlyOnceOptions.builder()
        .withTransactionPerConnection(true)
        .build(),
      new SerializableSupplier[XADataSource] {
        override def get(): XADataSource = {
          val xaDataSource = new MysqlXADataSource()
          xaDataSource.setUrl("jdbc:mysql://bigdata1:3306/test?useSSL=false&characterEncoding=utf8")
          xaDataSource.setUser("root")
          xaDataSource.setPassword("123456")
          return xaDataSource
        }
      }
    )
    res.addSink(MysqlExactlyOnceSink)*/


    env.execute()
  }
}
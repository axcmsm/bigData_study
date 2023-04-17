package sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import source.MyParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import java.util.UUID

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 写入数据到Hbase
 *
 * @author 须贺
 * @version 2023/4/16
 */
object HbaseSinkSample {
  def main(args: Array[String]): Unit = {
    //val configuration = new Configuration()
    //configuration.setInteger("rest.port", 8822) //默认是8081
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)
    val res = data.map(iter => {
      val arr = iter.split(",")
      Brand_Info(arr(0), arr(1), arr(2), arr(3))
    })
    res.print()

    // 这里采用 自定义Sink写入 hbase， 如果是在table中可以直接使用connector来进行写入
    res.addSink(new MyHbaseSink)

    env.execute()
  }
}

class MyHbaseSink extends RichSinkFunction[Brand_Info] {
  var connection: Connection = _
  var admin: Admin = _
  var table: Table = _

  override def open(parameters: Configuration): Unit = {
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "bigdata1:2181,bigdata2:2181,bigdata3:2181")
    connection = ConnectionFactory.createConnection(configuration)
    admin = connection.getAdmin
    val tableName = TableName.valueOf("brand_info1")
    if (!admin.tableExists(tableName)) {
      admin.createTable(
        new HTableDescriptor(tableName)
          .addFamily(new HColumnDescriptor("cf"))
      )
    }
    table = connection.getTable(tableName)
  }

  override def invoke(cf: Brand_Info, context: SinkFunction.Context): Unit = {
    //    val brandInfo =Brand_Info("001", "Apple", "888-888-8888", "Active")
    val put = new Put(Bytes.toBytes(cf.brand_id))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("brand_name"), Bytes.toBytes(cf.brand_name))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("telephone"), Bytes.toBytes(cf.telephone))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("brand_status"), Bytes.toBytes(cf.brand_status))
    table.put(put)
  }

  override def close(): Unit = {
    table.close()
    admin.close()
    connection.close()
  }
}
//configuration.set("hbase.zookeeper.property.clientPort", "2181") //会自动将其设置为 2181；
//：RPC 超时时间，默认值为 60s，作用是指定 HBase 客户端发送请求到服务端等待响应结束的时间
//configuration.set("hbase.rpc.timeout","10000")
//操作超时时间，默认值为 30s，作用是指定 HBase 客户端等待单个操作完成的时间；
//configuration.set("hbase.client.operation.timeout","30000") //规定时间内未响应,客户端会抛出 org.apache.hadoop.hbase.exceptions.TimeoutIOException 异常
//Scanner 超时时间，默认值为 10m，作用是指定 Scanner 等待获取数据的时间。
//configuration.set("hbase.client.scanner.timeout.period", "600000") //等待的时间超过了该参数设置的时间，Scanner 会被关闭,抛出 org.apache.hadoop.hbase.exceptions.ScannerTimeoutException 异常。

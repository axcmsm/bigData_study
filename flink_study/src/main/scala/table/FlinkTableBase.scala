package table

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import sink.Brand_Info
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object FlinkTableBase {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建 Table 环境
    val tableEnv = StreamTableEnvironment.create(env)
    //Table 对象获取方式:
    //1. 从已注册的表：（catalog_name.database_name.object_name） 默认的密码空间 default_catalog
    //2. 从 TableDescriptor（连接器/format/schema/options）:（表描述器，核心是 connector 连接器）
    //3. 从 DataStream ：（底层流）

    ////从已注册的表
    tableEnv.from("table1")

    //从 DataStream ：（底层流）
    // schema 根据样例类自动推断
    val info1 = Brand_Info("1", "name1", "telephone1", "status1")
    val info2 = Brand_Info("2", "name2", "telephone2", "status2")
    val dataStream = env.fromElements(info1, info2)
    tableEnv.fromDataStream(dataStream)
    // 手动推断
    /* val dataStream2 = env.fromElements(Seq(("1", "name1", "telephone1", "status1")),Seq(("2", "name2", "telephone2", "status2")))
     val table = tableEnv.fromDataStream(dataStream2,
       Schema.newBuilder()
         .column("f0", DataTypes.STRUCTURED(classOf[Brand_Info],
           DataTypes.FIELD("id", DataTypes.INT),
           DataTypes.FIELD("name", DataTypes.STRING),
           DataTypes.FIELD("telephone", DataTypes.STRING),
           DataTypes.FIELD("status", DataTypes.STRING)))
         .build()
     )*/
    /* 过 tableEnv 的 fromValues 方法获得 Table 对象（快速测试用）
     val table = tableEnv.fromValues(
          DataTypes.ROW(
            DataTypes.
              FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("info", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
            DataTypes.FIELD("ts1", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("ts3", DataTypes.TIMESTAMP_LTZ(3)),
            /*DataTypes.FIELD("ts5", DataTypes.TIMESTAMP_WITH_TIME_ZONE(3)),*/
          ), Row.of(1, "a", info, "2022-06-03 13:59:20.200",1654236105000L,"14:13:00.200")
        );*/

    //TableDescriptor
    //    tableEnv.createTable("t1", TableDescriptor.forConnector("filesystem")
    //      .option("path", "file:///d:/a.txt")
    //      .format("csv")
    //      .schema(Schema.newBuilder()
    //        .column("guid",DataTypes.STRING())
    //        .column("name",DataTypes.STRING())
    //        .column("age",DataTypes.STRING())
    //        .build())
    //      .build());

    tableEnv.executeSql(
      """
        |CREATE TABLE `brand_info` (
        |brand_id String,
        |brand_name String,
        |telephone String,
        |brand_web String,
        |brand_logo String,
        |brand_desc String,
        |brand_status String,
        |brand_order String,
        |modified_time String,
        |)
        |with
        |(
        |   'connector' = 'mysql-cdc',
        |   'hostname' = 'doit01',
        |   'port' = '3306',
        |   'username' = 'root',
        |   'password' = '123456',
        |   'database-name' = 'ds_db01',
        |   'table-name' = 'brand_info',
        |)
        |""".stripMargin)
    tableEnv.executeSql("select * from brand_info").print()

  }
}

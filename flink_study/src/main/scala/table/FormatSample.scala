package table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.TableDescriptor

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17
 */
object FormatSample {
  def main(args: Array[String]): Unit = {
    // 字段解析
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建 Table 环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 建表（数据源表）
    // {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
    tableEnv.sqlQuery(
      """
        |create table t_person(
        | id int , -- 物理字段
        | name string, -- 物理字段
        | nick string,age int, gender string ,guid as id, -- 表达式字段（逻辑字段）
        | big_age as age + 10 , -- 表达式字段（逻辑字段）
        | offs bigint metadata from 'offset' , -- 元数据字段
        | ts TIMESTAMP_LTZ(3) metadata from 'timestamp' -- 元数据字段
        | /*+ " PRIMARY KEY(id,name) NOT ENFORCED " */ -- 主键约束
        |
        |) WITH (
        |'connector' = 'kafka',
        |'topic' = 'test',
        |'properties.bootstrap.servers' = 'doitedu:9092',
        |'properties.group.id' = 'g1',
        |'scan.startup.mode' = 'earliest-offset',
        |'format' = 'json',
        |'json.fail-on-missing-field' = 'false',
        |'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)
    tableEnv.executeSql("desc t_person").print();
    tableEnv.executeSql("select * from t_person").print();


    // TODO: json format
    //format 组件名： json
    //json.fail-on-missing-field 缺失字段是否失败
    //json.ignor-parse-errors 是否忽略 json 解析错误
    //json.timestamp-format.standard json 中的 timestamp 类型字段的格式
    //json.map-null-key.mode 可取：FAIL ,DROP, LITERAL
    //json.map-null-key.literal 替换 null 的字符串
    //json.encode.decimal-as-plain-number


    //例如：{"id":10,"name":"nick","age":28,"regTime":"2022-06-06 12:30:30.100"}
    val schema = Schema.newBuilder()
      .column("id", DataTypes.INT())
      .column("name", DataTypes.STRING())
      .column("age", DataTypes.INT())
      .column("regTime", DataTypes.TIMESTAMP(3))
      .build()
    tableEnv.createTable("t1", TableDescriptor.forConnector("filesystem").option("path", "file:///d:/json.txt").format("json").schema(schema).build)
    tableEnv.executeSql("select id,name,age,regTime from t1").print

    //嵌套：{"id":10,"name":{"nick":"doe","formal":"doit edu"}}
    val schema1 = Schema.newBuilder()
      .column("id", DataTypes.INT())
      .column("name", DataTypes.ROW(
        DataTypes.FIELD("nick", DataTypes.STRING()),
        DataTypes.FIELD("formal", DataTypes.STRING())
      )
      )
      .build();

    //数组对象： {"id":1,"friends":[{"name":"a","info":{"addr":"bj","gender":"male"}},{"name":"b","info":{"addr":"sh","gender":"female"}}]}
    val schema3 = Schema.newBuilder()
      .column("id", DataTypes.INT())
      .column("friends", DataTypes.ARRAY(
        DataTypes.ROW(
          DataTypes.FIELD("name", DataTypes.STRING()), DataTypes.FIELD("info", DataTypes.ROW(
            DataTypes.FIELD("addr", DataTypes.STRING()), DataTypes.FIELD("gender", DataTypes.STRING())
          ))
        )))
      .build();

    // TODO: csv format
    //format = csv
    //csv.field-delimiter = ',' csv.disable-quote-character = false
    //csv.quote-character = ' " ' csv.allow-comments = false
    //csv.ignore-parse-erros = false 是否忽略解析错误
    //csv.array-element-delimiter = ' ; ' 数组元素之间的分隔符
    //csv.escape-character = none 转义字符
    //csv.null-literal = none null 的字面量字符串
    tableEnv.executeSql(
      """
        |create table t_csv(
        |id int,
        |name string,
        |age string
        |) with (
        | 'connector'='filesystem',
        | 'path'='data/csv',
        | 'format'='csv',
        | 'csv.disable-quote-character' = 'false',
        | 'csv.quote-character' = '|',
        | 'csv.ignore-parse-errors' = 'true',
        | 'csv.null-literal' = '\\N',
        | 'csv.allow-comments' = 'true'
        |)
        |""".stripMargin)
    tableEnv.executeSql("desc t_csv").print
    tableEnv.executeSql("select * from t_csv").print


  }
}

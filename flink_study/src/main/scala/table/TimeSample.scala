package table

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/17 
 */
object TimeSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    // TODO: 水位线提取及事件时间表
    // 方式一：先提取再转换成表

    // guid,uuid,eventId,pageId,ts
    val data = env.socketTextStream("master", 7777);
    // 换成 pojo 类型的 datastream
    val logEvent = data.map(s => {
      val arr = s.split(",")
      Log_Event(arr(0).toInt, arr(1), arr(2), arr(3).toLong)
    })
    // 分配 watermark 策略
    val ds = logEvent.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[Log_Event]()
        .withTimestampAssigner(new SerializableTimestampAssigner[Log_Event] {
          override def extractTimestamp(t: Log_Event, l: Long): Long = t.eventTime
        })
    )
    // 转成table
    val table = tableEnv.fromDataStream(
      ds, Schema.newBuilder()
        // 声明表达式字段，并声明为 processing time 属性字段
        .columnByExpression("pt", "proctime()")
        // 声明表达式字段（来自 ts）
        .columnByExpression("rt", "to_timestamp_ltz(ts,3)")
        // 将 rt 字段指定为 event time 属性字段，并基于它指定 watermark 策略： = rt
        .watermark("rt", "rt")
        // 将 rt 字段指定为 event time 属性字段，并基于它指定 watermark 策略： = rt - 8s
        .watermark("rt", "rt - interval '8' second")
        // 将 rt 字段指定为 event time 属性字段，并沿用“源头流”的 watermark
        .watermark("rt", "SOURCE_WATERMARK()") // 得到与源头 watermark 完全一致
        .build()
    )
    table.printSchema()

    //方式二：在表中定义
    //1. 先设法定义一个 timestamp(3)或者 timestamp_ltz(3)类型的字段（可以来自于数据字段，也可以来自于一个元数据： rowtime
    //rt as to_timestamp_ltz(eventTime) -- 从一个 bigint 得到一个 timestamp（3）类型的字段
    //rt timestamp(3) metadata from 'rowtime' -- 从一个元数据 rowtime 得到一个 timestamp(3)类型的字段

    //2. 然后基于该字段，用 watermarkExpression 声明 watermark 策略
    //watermark for rt AS rt- interval '1' second
    //watermark for rt AS source_watermark() -- 代表使用底层流的 watermark 策略


    // 处理时间表
    //方式一：
    // 转成 table Table
    val table2 = tableEnv.fromDataStream(ds, Schema.newBuilder()
      // ....字段
      // 声明表达式字段，并声明为 processing time 属性字段
      .columnByExpression("pt", "proctime()")
      .build()
    )
    //方式二：
    //pt as proctime()


  }
}

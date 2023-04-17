package source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 读取kafka数据 版本一
 * @author 须贺
 * @version 2023/4/15 
 */
object ReadKafkaData_2 {
   def main(args: Array[String]): Unit = {
     val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setParallelism(1)

     //测试数据：7,ViVo,13056955247,0
     val data = env.fromSource(
       KafkaSource.builder[String]()
         .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdata3:9092")
         .setTopics("brand_info")
         .setGroupId("ReadKafkaData_2")
         .setValueOnlyDeserializer(new SimpleStringSchema())
         .setStartingOffsets(OffsetsInitializer.earliest()) //从头开始消费
         .build(),
       WatermarkStrategy.noWatermarks(),
       "ReadKafkaData_2_Source"
     )
     data.print()

     env.execute("ReadKafkaData_2")
   }
}

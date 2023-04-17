package source

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 读取kafka数据 版本一
 * @author 须贺
 * @version 2023/4/15 
 */
@Deprecated //已过期
@PublicEvolving
object ReadKafkaData_1 {
   def main(args: Array[String]): Unit = {
     val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setParallelism(1)

     //测试数据：7,ViVo,13056955247,0
     val props = new Properties()
     props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"bigdata1:9092,bigdata2:9092,bigdata3:9092")
     props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ReadKafkaData_1")
     props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer].getName)
     props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer].getName)
     val consumer = new FlinkKafkaConsumer[String]("ods_mall_data", new SimpleStringSchema(), props)
     consumer.setStartFromEarliest()//从头开始消费
     //consumer.setStartFromLatest()//最新开始消费

     val data = env.addSource(consumer)
     data.print()

     env.execute("ReadKafkaData_1")
   }
}

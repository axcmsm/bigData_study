package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import source.MyParallelSource

import java.util.Properties

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object KafkaSink_2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)


    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers("bigdata1:9092,bigdata2:9092,bigdata3:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[String]()
          .setTopic("brand_info")
          //.setTopicSelector() //主题选择器
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) //最少一次
      .setTransactionalIdPrefix("bigdata1_") //为第一个实例设置前缀,用于设置事务 ID 的前缀,
      .build()

    //setTransactionalIdPrefix
    // 如果不指定,随机的事务 ID。如果多个实例使用同一个 KafkaSink，可能会出现事务 ID 冲突的情况。
    // 为了避免这种情况，可以手动设置x，并在每个实例中加上不同的后缀，以确保每个实例的事务 ID 均不相同。
    data.sinkTo(kafkaSink)
    env.execute()
  }
}

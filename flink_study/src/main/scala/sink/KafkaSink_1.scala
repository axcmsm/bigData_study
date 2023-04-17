package sink

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
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
@deprecated
@PublicEvolving //老版本
object KafkaSink_1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)

    val topic = "brand_info"
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata1:9092,bigdata2:9092,bigdata3:9092")
    val KafkaSink = new FlinkKafkaProducer[String](
      topic, new SimpleStringSchema(),
      properties
    )
    data.addSink(KafkaSink)


    env.execute()
  }
}

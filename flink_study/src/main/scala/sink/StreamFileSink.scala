package sink

import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.hive.shaded.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyParallelSource
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import transform.Brand_Event
import org.apache.flink.hive.shaded.formats.parquet.ParquetWriterFactory
import org.apache.flink.hive.shaded.formats.parquet.avro.ParquetAvroWriters

import scala.util.Random
/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object StreamFileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1).map(s=>{
      val arr = s.split(",")
      val randomAmount = Random.nextInt(100) + 1
      Brand_Event(arr(0), "【" + arr(1) + "】", arr(2), arr(3),randomAmount)
    })
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointStorage("file:///e:/ckpt")
    //该 Sink 不但可以将数据写入到各种文件系统中，而且整合了 checkpoint 机制来保证 Exacly Once 语义，
    //还可以对文件进行分桶存储，还支持以列式存储的格式写入，功能更强大
    /*
     //方式一
     //Row 格式文件输出代码示例
        val rollingPolicy = DefaultRollingPolicy.create()
          .withRolloverInterval(30 * 1000L) //30 秒滚动生成一个文件
          .withMaxPartSize(1024L * 1024L * 100L) //当文件达到 100m 滚动生成一个文件
          .build()
        //创建 StreamingFileSink，数据以行格式写入
        val outputPath=""
        val sink = StreamingFileSink.forRowFormat(
          new Path(outputPath), //指的文件存储目录
          new SimpleStringEncoder[String]("UTF-8")) //指的文件的编码
          .withRollingPolicy(rollingPolicy) //传入文件滚动生成策略
          .build()
        data.addSink(sink)*/

    //方式二：
    //Bulk 列式存储文件输出

    val parquetWriterFactory = ParquetAvroWriters.forReflectRecord(classOf[Brand_Event])
    val bulkSink = FileSink.forBulkFormat(new Path("e:/datasink3/"),
      parquetWriterFactory)
      .withBucketAssigner(new DateTimeBucketAssigner[Brand_Event]("yyyy-MM-dd--HH"))
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("axcmsm").withPartSuffix(".parquet").build())
      .build();
    data.sinkTo(bulkSink)
  }
}

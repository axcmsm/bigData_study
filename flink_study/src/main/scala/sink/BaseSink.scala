package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyParallelSource

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 * 基础的Sink
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object BaseSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data = env.addSource(new MyParallelSource).setParallelism(1)

    //打印到控制台，
    //val printFunction= new PrintSinkFunction[String]
    //data.addSink(printFunction).name("打印到控制台")
    data.print() //底层就是调用上面的逻辑

    //写入csv文件
    //data.writeAsText("/data")//FileSystem.WriteMode.OVERWRITE

    //指定格式输出
   //data.writeUsingOutputFormat(new TextOutputFormat[String](new Path("/data")))


    //写入socket
    data.writeToSocket("master", 7777, new SimpleStringSchema())

    env.execute()
  }
}

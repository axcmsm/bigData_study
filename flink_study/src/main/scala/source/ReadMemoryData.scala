package source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object ReadMemoryData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(1, 2, 3, 4).print("fromElements")

    env.fromCollection(Seq( 4, 5,6,7,8)).print("fromCollection")

    env.execute("ReadMemoryData")
  }
}

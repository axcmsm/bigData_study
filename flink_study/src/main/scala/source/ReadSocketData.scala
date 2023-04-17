package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object ReadSocketData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    //val data=env.addSource(new SocketTextStreamFunction(host, port,"\n",0))
    val data = env.socketTextStream(host, port)
    data.print("data")
      
    env.execute("ReadSocketData")
    // --host master --port 7777
    // nc -lk 7777
  }
}

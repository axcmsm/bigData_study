package source

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/15 
 */
object ReadFileData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val url = this.getClass.getResource("/file.txt").getPath
    //PROCESS_ONCE，只读取文件中的数据一次，读取完成后，程序退出
    //PROCESS_CONTINUOUSLY，会一直监听指定的文件，文件的内容发生变化后，会将以前的内容和新的内容全部都读取出来，进而造成数据重复读取
    val data = env.readFile(new TextInputFormat(null),url,FileProcessingMode.PROCESS_CONTINUOUSLY,2000)
//    val data = env.readTextFile(url)
    data.print()
    env.execute()
  }
}

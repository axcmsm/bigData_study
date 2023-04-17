package basic

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/12
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 设置执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 加载数据源
    val text = env.readTextFile("data.txt")

    // 调用 flatMap 方法进行单词切分
    val words = text.flatMap(line => line.split(" "))

    // 调用 map 方法将单词转换为 (word, 1) 格式
    val pairs = words.map(word => (word, 1))

    // 调用 groupBy 方法对单词进行分组
    val grouped = pairs.groupBy(0)

    // 调用 sum 方法对单词出现次数进行累加
    val counts = grouped.sum(1)

    // 输出结果
    counts.print()
  }
}

package table

/**
 * this class is for Axcmsm
 * 微信公众号：代码飞快
 *
 * @author 须贺
 * @version 2023/4/18 
 */
object SelectExSample {
  def main(args: Array[String]): Unit = {
    //表查询案例
    //基本查询
    //高阶聚合
    //group by cube(维度 1，维度 2，维度 3)
    //group by grouping sets( (维度 1，维度 2) ,(维度 1，维度 3), (维度 2)，())
    //group by rollup(省，市，区)

    //时间窗口
    //1. 在窗口上做分组聚合，必须带上 window_start 和 window_end 作为分组 key
    //2. 在窗口上做 topN 计算 ，必须带上 window_start 和 window_end 作为 partition 的 key.
    // 3. 带条件的 join，必须包含 2 个输入表的 window start 和 window end 等值条件.
    //滚动窗口 （Tumble Windows）:TUMBLE(TABLE t_action，DESCRIPTOR(时间属性字段) , INTERVAL '10' SECONDS[ 窗口长度 ] ）
    //滑动窗口 （Hop Windows）: HOP(TABLE t_action, DESCRIPTOR(时间属性字段) , INTERVAL '5' SECONDS[ 滑动步长 ], INTERVAL '10' SECONDS[ 窗口长度 ] )
    //累计窗口（Cumulate Windows）: CUMULATE(TABLE t_action, DESCRIPTOR(时间属性字段) , INTERVAL '5' SECONDS[ 更新步长 ], INTERVAL '10' SECONDS[ 窗口最大长度 ] )

    /**
     * select window_start, window_end, channel, count(distinct guid) as uv
     * from table(
     * tumble(table t_applog,descriptor(rw), interval '5' minutes)
     * )
     * group by window_start,window_end,channel
     */


    //关联查询
    //window join
    //regular join
    //lookup join
    //interval join
    //temporal join


  }
}

package com.imqk.spark.api.java.Sql;

/**
 * Created by 428900 on 2017/5/13.
 *
 * 统计搜索排名前5的产品
 *
 * 元数据： Date, userID, Item, City, Device
 *
 * 处理步骤
 * 1, 原始的ETL，过滤数据后产生目标，使用 RDD FILTER 操作，广播等
 * 2，过滤后的数据进行指定条件的查询；并对数据 FILTER，广播等
 * 3，由于商品是分种类的，得出结论之前，首先会基于商品进行 UV，对用户来说，则也需要统计 PV
 *      对UV进行计算的话，必须构建 K－V的RDD --> (date#Item,uesrID)，以便进行 GROUPBYKEY操作
 *      在调用了GROUPBYKEY后，对USER去重，并计算出每一种商品的UV，结果数据类型为(date#Item,uesrID)
 * 4, 使用开窗函数 ROW_NUMBER 分组并统计出 UV 排名前5的商品 ：
 *      row_number() over (PARTITIONED BY date ORDER BY uv DESC) RANK
 *      此时会产生以 date, item, uv 为一条具体内容的 ROW 的　DF
 * 5, DF 转 RDD 根据日期进行分组并分析出每天排名前5的热搜索ITEM
 * 6, 进行KEY－VALUE交换，调用 sortByKey 进行点击热度排名
 * 7, 再次进行K－V交换，得出目标数据库（date#Item,UV）的格式
 * 8， 通过RDD直接操作MYSQL等把结果放入生产系统的DB中，再通过JAVA EE等提供可视化结果
 *      也可以存储在HIVE、SPARK SQL等处，通过Thrift 提供给其它系统使用
 *      速度要求高时，也可以放在 Redis 中。
 *
 */
public class Top5BySparkSQL {
    public static void main(String[] args) {

    }
}

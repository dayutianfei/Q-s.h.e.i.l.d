/**
 * 针对存储在HDFS中的小文件进行测试
 * 测试场景分成两个部分：
 * 1. 小文件直接存储在HDFS中，使用多级目录进行组织，模拟按照表、分区进行存放；
 * 2. 将多个小文件构造成一个大文件，按照1中的存放方式进行数据组织；
 */
/**
 * @author egret
 *
 */
package cn.dayutianfei.hdfs.smallfile;
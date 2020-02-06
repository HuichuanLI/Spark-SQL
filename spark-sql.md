---
description: 李汇川探索Spark 至Spark SQL
---

# Spark SQL

## MapReduce 槽点
	需求：统计单词出现的个数（词频统计）
	file中每个单词出现的次数
		hello,hello,hello
		world,world
		pk
		1）读取file中每一行的数据
		2) 按照分隔符把每一行的内容进行拆分
		3）按照相同的key分发到同一个任务上去进行累加的操作
		
		
	这是一个简单的不能再简单的一个需求，我们需要开发很多的代码
		1）自定义Mapper
		2）自定义Reducer
		3）通过Driver把Mapper和Reducer串起来
		4）打包，上传到集群上去
		5）在集群上提交我们的wc程序		
	一句话：就是会花费非常多的时间在非业务逻辑改动的工作上	

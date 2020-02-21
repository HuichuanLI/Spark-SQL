# Spark SQL

## MapReduce 槽点

### 吐槽点1
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

### 吐槽点2

    Input => MapReduce ==> Output ==> MapReduce ==> Output
    	回顾下MapReduce执行流程：
    		MapTask或者ReduceTask都是进程级别
    		第一个MR的输出要先落地，然后第二个MR把第一个MR的输出当做输入
    		中间过程的数据是要落地

## Spark特性
    1）Speed
        both batch and streaming data
        批流一体 Spark Flink

        快：从哪些角度来知道快呢？
        1、消除了冗余的HDFS读写
        Hadoop每次shuffle操作后，必须写到磁盘，而Spark在shuffle后不一定落盘，可以cache到内存中，以便迭代时使用。如果操作复杂，很多的shufle操作，那么Hadoop的读写IO时间会大大增加。
        
        2、消除了冗余的MapReduce阶段
        Hadoop的shuffle操作一定连着完整的MapReduce操作，冗余繁琐。而Spark基于RDD提供了丰富的算子操作，且reduce操作产生shuffle数据，可以缓存在内存中。
        
        3、JVM的优化
        Hadoop每次MapReduce操作，启动一个Task便会启动一次JVM，基于进程的操作。而Spark每次MapReduce操作是基于线程的，只在启动Executor是启动一次JVM，内存的Task操作是在线程复用的。
        
        4、Spark是基于内存进行数据处理的，MapReduce是基于磁盘进行数据处理的
        
        5.spark 基于DAG模式的
    
    2）Ease of Use
    	high-level operators	
    
    3）Generality
    	stack  栈   生态
    
    4）Runs Everywhere
        It can access diverse data sources
        YARN/Local/Standalone Spark应用程序的代码需要改动吗？
        --master来指定你的Spark应用程序将要运行在什么模式下
     
### Spark 运行模式
    
- local：本地运行，在开发代码的时候，我们使用该模式进行测试是非常方便的
- standalone：Hadoop部署多个节点的，同理Spark可以部署多个节点  用的不多
- YARN：将Spark作业提交到Hadoop(YARN)集群中运行，Spark仅仅只是一个客户端而已
- Mesos
- K8S：2.3版本才正式稍微稳定   是未来比较好的一个方向
- 补充：运行模式和代码没有任何关系，同一份代码可以不做修改运行在不同的运行模式下

### MVN+IDEA 生成文件

#### pom.xml
    mvn archetype:generate -DarchetypeGroupId=net.alchim31.maven \
    -DarchetypeArtifactId=scala-archetype-simple \
    -DremoteRepositories=http://scala-tools.org/repo-releases \
    -DarchetypeVersion=1.5 \
    -DgroupId=com.bigdata \
    -DartifactId=sparksql-train \
    -Dversion=1.0
    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <encoding>UTF-8</encoding>
        <scala.tools.version>2.11</scala.tools.version>
        <scala.version>2.11.8</scala.version>
        <spark.version>2.4.3</spark.version>
        <hadoop.version>2.6.0-cdh5.15.1</hadoop.version>
    </properties>	
    
    添加CDH的仓库
    <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
        </repository>
    </repositories>
    
    添加Spark SQL和Hadoop Client的依赖
    <!--Spark SQL依赖-->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
        <version>${spark.version}</version>
    </dependency>
    
    <!-- Hadoop相关依赖-->
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>${hadoop.version}</version>
    </dependency>
### 词频统计案例
    输入：文件
    需求：统计出文件中每个单词出现的次数
    	1）读每一行数据
    	2）按照分隔符把每一行的数据拆成单词
    	3）每个单词赋上次数为1
    	4）按照单词进行分发，然后统计单词出现的次数
    	5）把结果输出到文件中
    输出：文件
    
      spark-submit --class main.scala.com.bigdata.SparkWordCountAppV2 
    --master local /Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/target/ sparksql-train-1.0.jar
    file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/input2.txt file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/output/

     spark-submit --class main.scala.com.bigdata.SparkWordCountAppV2 --master yarn /Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/target/sparksql-train-1.0.jar hdfs://localhost:8020/data/input2.txt file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/output/

    standalone 模式
    
## Hadoop vs Spark

![Hadoop vs Spark](./photo/01.png)

![Hadoop vs Spark](./photo/02.png)

![Hadoop vs Spark](./photo/03.png)



## Spark-SQL 快速入门
     为什么需要SQL？
         事实上的标准
             MySQL/Oracle/DB2...  RBDMS 关系型数据库  是不是过时呢？
             数据规模  大数据的处理
     
             MR：Java
             Spark：Scala、Java、Python
     
             直接使用SQL语句来对数据进行处理分析呢？  符合市场的需求
                 Hive SparkSQL Impala...
     
         受众面大、容易上手、易学易用
             DDL DML
     
             access.log
             1,zhangsan,10,beijing
             2,lisi,11,shanghai
             3,wangwu,12,shenzhen
     
             table: Hive/Spark SQL/Impala   共享元数据
                 name: access
                 columns: id int,name string,age int,city string
             SQL: select xxx from access where ... group by ... having....
        
        SQL on Hadoop
            使用SQL语句对大数据进行统计分析，数据是在Hadoop
        
            Apache Hive
                SQL转换成一系列可以在Hadoop上运行的MapReduce/Tez/Spark作业
                SQL到底底层是运行在哪种分布式引擎之上的，是可以通过一个参数来设置
                功能：
                    SQL：命令行、代码
                    多语言Apache Thrift驱动
                    自定义的UDF函数：按照标准接口实现，打包，加载到Hive中
                    元数据
        
            Cloudera Impala
                使用了自己的执行守护进程集合，一般情况下这些进程是需要与Hadoop DN安装在一个节点上
                功能：
                    92 SQL支持
                    Hive支持
                    命令行、代码
                    与Hive能够共享元数据
                    性能方面是Hive要快速一些，基于内存
        
            Spark SQL
                Spark中的一个子模块，是不是仅仅只用SQL来处理呢？
                Hive：SQL ==> MapReduce
                Spark：能不能直接把SQL运行在Spark引擎之上呢？
                    Shark： SQL==>Spark      X
                        优点：快  与Hive能够兼容
                        缺点：执行计划优化完全依赖于Hive  MR进程 vs Spark线程
                        使用：需要独立维护一个打了补丁的Hive源码分支
        
                ==>
                    1） Spark SQL
                        SQL，这是Spark里面的
                    2） Hive on Spark
                        Hive里面的，通过切换Hive的执行引擎即可，底层添加了Spark执行引擎的支持
        
        
            Presto
                交互式查询引擎  SQL
                功能
                    共享元数据信息
                    92语法
                    提供了一系列的连接器，Hive Cassandra...
        
            Drill
                HDFS、Hive、Spark SQL
                支持多种后端存储，然后直接进行各种后端数据的处理
        
        
            Phoenix
                HBase的数据，是要基于API进行查询
                Phoenix使用SQL来查询HBase中的数据
                主要点：如果想查询的快的话，还是取决于ROWKEY的设计
## Spark SQL是什么
    Spark SQL is Apache Spark's module for working with structured data.
    误区一：Spark SQL就是一个SQL处理框架
    
    1）集成性：在Spark编程中无缝对接多种复杂的SQL
    2）统一的数据访问方式：以类似的方式访问多种不同的数据源，而且可以进行相关操作
        spark.read.format("json").load(path)
        spark.read.format("text").load(path)
        spark.read.format("parquet").load(path)
        spark.read.format("json").option("...","...").load(path)
    3) 兼容Hive
            allowing you to access existing Hive warehouses
            如果你想把Hive的作业迁移到Spark SQL，这样的话，迁移成本就会低很多
    4）标准的数据连接：提供标准的JDBC/ODBC连接方式   Server
    
    Spark SQL应用并不局限于SQL
    还支持Hive、JSON、Parquet文件的直接读取以及操作
    SQL仅仅是Spark SQL中的一个功能而已
    
## 为什么要学习Spark SQL
        SQL带来的便利性
        Spark Core： RDD  Scala/Java
            熟悉Java、Scala语言，不然你也开发不了代码， 入门门槛比较大，学习成本比较大
        Spark SQL
            Catalyst 为我们自动做了很多的优化工作
            SQL(只要了解业务逻辑，然后使用SQL来实现)
            DF/DS：面向API编程的，使用一些Java/Scala

## Spark SQL架构
       Frontend
           Hive AST   : SQL语句（字符串）==> 抽象语法树
           Spark Program : DF/DS API
           Streaming SQL
       Catalyst
           Unresolved LogicPlan
               select empno, ename from emp
           Schema Catalog
               和MetaStore
   
           LogicPlan
   
           Optimized LogicPlan
               select * from (select ... from xxx limit 10) limit 5;
               将我们的SQL作用上很多内置的Rule，使得我们拿到的逻辑执行计划是比较好的
   
           Physical Plan
       Backend

## Spark-shell
    spark-shell
        每个Spark应用程序（spark-shell）在不同目录下启动，其实在该目录下是有metastore_db
        单独的
        如果你想spark-shell共享我们的元数据的话，肯定要指定元数据信息==> 后续讲Spark SQL整合Hive的时候讲解
        spark.sql(sql语句)
    
    
    spark-sql的使用
        spark-shell你会发现如果要操作SQL相关的东西，要使用spark.sql(sql语句)
        
    spark-shell启动流程分析
    REPL: Read-Eval-Print Loop  读取-求值-输出
    提供给用户即时交互一个命令窗口
    
    通过讲解spark-shell的启动流程，是想向小伙伴们传递一个信息：论shell在大数据中的重要性
    
    spark-sql执行流程分析
        spark-sql底层调用的也是spark-submit
        因为spark-sql它就是一个Spark应用程序，和spark-shell一样
        对于你想启动一个Spark应用程序，肯定要借助于spark-submit这脚本进行提交
        spark-sql调用的类是org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
        spark-shell调用的类是org.apache.spark.repl.Main



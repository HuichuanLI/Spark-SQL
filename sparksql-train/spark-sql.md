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

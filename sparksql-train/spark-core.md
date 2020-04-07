## Spark

### MapReduce的局限性：
- 1）代码繁琐；
- 2）只能够支持map和reduce方法；
- 3）执行效率低下；
- 4）不适合迭代多次、交互式、流式的处理；

### 框架多样化：
- 1）批处理（离线）：MapReduce、Hive、Pig
- 2）流式处理（实时）： Storm、JStorm
- 3）交互式计算：Impala

学习、运维成本无形中都提高了很多

## bin/spark-shell运行模式 
        默认情况下，表示运行在local mode，在本地启动一个JVM Process，在里面运行一些线程进行数据处理，每个线程运行一个Task任务。
        思考：每个JVM Process中运行多少个线程Thread呢？？
    对于本地模式来说，运行多少个Thread决定同事运行多少Task。
        -1. bin/spark-sell --master local 
            在JVM中运行一个Thread
        -2. bin/spark-sell --master local[K] 
            K: 表示运行K个Thread
        -3. bin/spark-sell --master local[*] 
            *: 表示的是当前机器CPU CORE为多少就运行多少个Thread。
    通常情况下：
        $ bin/spark-shell --master local[2]
            表明运行2个Thread进行数据处理，也就是说可以同事有两个Task进行数据处理，相当并行计算。
            
    spark-shell 就是一个spark-application会有很多的job
    重要一点：无论是MR还是Spark对数据分析处理，都是分而治之的思想，处理每一份数据，每个数据都是一个Task进行处理。
    MR： MapTask和ReduceTask -> JVM Process 
    Spark: Task -> Thread 
    每个Task运行的时候，仅仅需要一个CPU CORE即可。

## Standalone
    - 对于YARN来说：
        每天机器上只能启动一个NodeManager，管理这个节点的资源
    - 对于Standalone来说：
        每台机器上可以运行多个Workder进程，进行资源管理
        
    配置Spark Standalone Cluster 
    -1. 配置${SPARK_HOME}/conf目录下的文件
        - spark-env.sh 
            SPARK_MASTER_IP=bigdata-training01.hpsk.com
            SPARK_MASTER_PORT=7077
            SPARK_MASTER_WEBUI_PORT=8080
            SPARK_WORKER_CORES=2
            SPARK_WORKER_MEMORY=2g
            SPARK_WORKER_PORT=7078
            SPARK_WORKER_WEBUI_PORT=8081
            SPARK_WORKER_INSTANCES=1
        - slaves
            bigdata-training01.hpsk.com
    -2. 启动服务
        Master启动：
            必须在Master节点机器上启动
            $ sbin/start-master.sh 
        Workers启动：
            必须在Master节点机器上启动
            启动之前，需要配置Master节点到所有Slaves节点的SSH无密钥登录
                由于在Master节点通过远程SSH登录启动各个节点上的Worker进程
                    $ ssh-keygen -t rsa
                    $ ssh-copy-id hpsk@bigdata-training01.hpsk.com
            $ sbin/start-slaves.sh 
            
    -3. 监控测试
        -3.1 WEB UI 
            http://master-ip:8080/
        -3.2 运行spark-shell 
            运行spark-shell在SparkStandalone集群模式下
            $ bin/spark-shell --master spark://bigdata-training01.hpsk.com:7077
    -4. Spark Application 运行在Cluster模式下
        以运行在SparkStandaloneCluster为例
        组成部分：
        - Driver Program
            类似于MapReduce运行在YARN上，每个应用都有一个AppMaster
            管理整个应用的运行（调度）
            SparkContext 创建在此处
                - 读取要处理的数据
                - 调度整个Application中所有的Job
            默认情况下：Driver Program运行在提交Application的客户端，而不是集群的某台机器上。
            进程名：SparkSubmit
        - Executors
            JVM Process，运行在Worker节点上，类似于MapReduce中MapTask和ReduceTask运行在NodeManager上
            -i, 运行Task 进行数据处理，以Thread方式运行
            -ii, 缓存数据：rdd.cache，其实就是讲数据放到Executor内存中
            进程名：CoarseGrainedExecutorBackend
    
    预先补充知识：
        Spark分析数据：将要处理的数据封装到 数据结构 中-> 
            RDD 集合：内存中（理想），分开放->分区
        假设要处理的数据为5GB，存储在HDFS上，比如1000blocks
            默认情况下 blocks  ->  partition  -> Task 
        总结：
            Spark处理数据思想：
                将要处理的数据封装到一个集合RDD中，分成很多份（分区），每一份（每一分区）数据被一个Task进行处理。

    Spark Application 
        Job-01
            Stage-01
                Task-01
                Task-02
                ......
            Stage-02
            Stage-03
            ......
        Job-02
        Job-03
        ......
        
    -1, 一个Application包含多个Job
        RDD#Func 
            当RDD调用的函数返回值不是RDD的时候，就会触发一个Job
        Func:Action
    -2, 一个Job中包含很多Stage，Stage之间是依赖关系
        后面的Stage处理的数据依赖于前面Stage处理完成的额数据
        Stage里面都是RDD进行转换操作
    -3, 一个Stage中有很多Task
        在一个Stage中所有的Task来说，处理的数据逻辑相同（代码相同），仅仅是处理的数据不同。
        一个Stage中有多少Task，取决于处理数据RDD的分区数目，每个分区的数据由一个Task任务进行处理。

    
    查看spark-shell脚本
        "${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
    Spark中所有的应用Application都是通过
        spark-submit进行提交的
        
### spark 部署模式
    --deploy-mode 
        DEPLOY_MODE
            Whether to launch the driver program locally ("client") or on one of the worker machines inside the cluster ("cluster")(Default: client).
        一言以蔽之：
            SparkApplication运行时Driver Program运行的地方（针对将Spark Application运行在Cluster环境中）
            有两种方式：
            -1, client
                提交应用的地方（执行spark-submit脚本的地方），
                就在那台机器提交application应用所在的机器上运行，
                启动JVM Process。
            -2, cluster 
                运行在Cluster的从节点上
                    SparkStandalone：运行在Worker节点
                    YARN：NodeManager节点
        建议：
            在实际项目企业中
            - client：
                程序开发、测试、性能调用
                会在client端显示日志信息，便于我们观察监控程序运行
            - cluster：
                生产集群采用此方式
                不需要及时查看日志信息（Job调度相关信息），除非出问题，可以再去节点查看。
                
                
## RDD的核心概念

    Spark核心抽象：
        数据结构 RDD
        - 官方文档：
            http://spark.apache.org/docs/1.6.1/programming-guide.html
    a resilient distributed dataset (RDD)
        which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. 
        - 源码说明：
    Represents an immutable,partitioned collection of elements that can be operated on in parallel.
    
    Internally, each RDD is characterized by five main properties:
     *
     *  - A list of partitions
        protected def getPartitions: Array[Partition]
        每个RDD有一些分区组成
        类似于SCALA中List集合，不同的是分为多个部分
     *  - A function for computing each split
        def compute(split: Partition, context: TaskContext): Iterator[T]
        每个分片的数据应用一个函数进行处理
        函数基本上都是高阶函数 类似于SCALA中List中的函数
     *  - A list of dependencies on other RDDs
        protected def getDependencies: Seq[Dependency[_]] = deps
        每个RDD依赖于一些列的RDD
        lineage
     *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
        RDD[(Key, Value)] 当RDD的数据类型为二元组的时候，可以指定数据分区
        分区器Partitioner，默认采用HashPartitioner
     *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
       an HDFS file)
        最优位置：针对RDD中分区的数据处理的时候
        比如要处理一个分区的数据，此时有以下几种存储的位置
        preferred locations    
    
    
    分区partition
        从数据存储的角度来看
    分片split
        从数据处理的角度来看


### 创建RDD

    在SparkCore中，创建RDD两种方式
        - 并行化集合
            sc.parallelize(List(1,2,3,4,5))
            
        - 从HDFS/Local System等支持HADOOP FS地方读取
            sc.textFile("")
    
    
### RDD的persisit
    对于RDD的Persistence来说
        - 持久化  属于lazy，需要RDD.action进行触发
            rdd.cache()  // 最理想，将数据分析都放入内存中
        - unpersist
            eager  立即生效
        - persist
            rdd.cache 考虑一点，将数据放到内存中，Eexcutor内存
            要考虑内存够大呀？？
            
    什么情况下将RDD进行持久化？？
        - 当某个RDD需要被多次使用的时候，应该考虑进行cache
        - 当某个RDD通过比较复杂的计算得来的时候，使用不止一次，考虑进行chache
     
    常用的RDD的Transformation和Action
        - join()
            关联，类似于SQL或者MapReduce Join，字段
            RDD[(Key, Value)], 依据Key进行关联
        - union()
            合并
            要求合并的RDD的类型要一致
        - mapPartitions()
            功能类似于map函数，不一样的是
            map：对RDD中的每个元素应用函数
                map(item => func(item))
            mapPartitions: 对RDD中的每个分区应用函数
                mapPartitions(iter => func(iter))
    
        - coalesce()/repartition()
            调整RDD的分区数目
            def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
            在实际开发中什么时候需调整分区数目呢？？
            - 增加RDD分区
                不多，考虑并行度
                一般情况下，在创建RDD的时候就会指定合适的分区数目
            - 减少RDD分区
                为什么呢？？？ 一个分区对应一个Task进行处理
                如果分区中的数据较少，甚至有的分区没有数据，运行Task是需要时间的，效率低下
        - 输出函数foreach()/foreachPartition()     
            foreach():
                针对每个元素进行调用函数，与map函数类似
            foreachPartition() ：  -> 使用此比较多
                针对每个分区的数据进行代用函数，与mapPartitions类似
        - 重要的重点API（作为作业）
            class PairRDDFunctions[K, V](self: RDD[(K, V)])
                - combineByKey
                - aggregateByKey
                - foldByKey
                功能与reduceByKey差不多，但是企业实际开发中往往使用上述的三个函数
                
    常用的RDD的Transformation和Action
        - join()
            关联，类似于SQL或者MapReduce Join，字段
            RDD[(Key, Value)], 依据Key进行关联
        - union()
            合并
            要求合并的RDD的类型要一致
        - mapPartitions()
            功能类似于map函数，不一样的是
            map：对RDD中的每个元素应用函数
                map(item => func(item))
            mapPartitions: 对RDD中的每个分区应用函数
                mapPartitions(iter => func(iter))
    
        - coalesce()/repartition()
            调整RDD的分区数目
            def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
            在实际开发中什么时候需调整分区数目呢？？
            - 增加RDD分区
                不多，考虑并行度
                一般情况下，在创建RDD的时候就会指定合适的分区数目
            - 减少RDD分区
                为什么呢？？？ 一个分区对应一个Task进行处理
                如果分区中的数据较少，甚至有的分区没有数据，运行Task是需要时间的，效率低下
        - 输出函数foreach()/foreachPartition()     
            foreach():
                针对每个元素进行调用函数，与map函数类似
            foreachPartition() ：  -> 使用此比较多
                针对每个分区的数据进行代用函数，与mapPartitions类似
        - 重要的重点API（作为作业）
            class PairRDDFunctions[K, V](self: RDD[(K, V)])
                - combineByKey
                - aggregateByKey
                - foldByKey
                功能与reduceByKey差不多，但是企业实际开发中往往使用上述的三个函数
                
        需求：
            先按照第一个字段进行分组且排序，在组内按照第二个字段进行排序并且获取各个组内前几个值Top3.
        
        分组的时候，肯定将相同Key的值分发到一个分区中，也就是被一个Task进行处理，如果某个组内的数据比较多，在一个Task中进行排序，将会出现数据倾斜，可能出现运行很慢或者内存不足排序出错。
            分阶段进行聚合操作（分阶段进行排序）
            
         需求：
             先按照第一个字段进行分组且排序，在组内按照第二个字段进行排序并且获取各个组内前几个值Top3.
         
         分组的时候，肯定将相同Key的值分发到一个分区中，也就是被一个Task进行处理，如果某个组内的数据比较多，在一个Task中进行排序，将会出现数据倾斜，可能出现运行很慢或者内存不足排序出错。
             分阶段进行聚合操作（分阶段进行排序）
             
             
### Spark的基础概念
    Application	：基于Spark的应用程序 =  1 driver + executors
    		User program built on Spark. 
    		Consists of a driver program and executors on the cluster.
    		spark0402.py
    		pyspark/spark-shell
    
    	Driver program	
    		The process running the main() function of the application 
    		creating the SparkContext	
    
    	Cluster manager
    		An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN)	
    		spark-submit --master local[2]/spark://hadoop000:7077/yarn
    
    	Deploy mode	
    		Distinguishes where the driver process runs. 
    			In "cluster" mode, the framework launches the driver inside of the cluster. 
    			In "client" mode, the submitter launches the driver outside of the cluster.	
    
    	Worker node	
    		Any node that can run application code in the cluster
    		standalone: slave节点 slaves配置文件
    		yarn: nodemanager
    
    
    	Executor	
    		A process launched for an application on a worker node
    		runs tasks 
    		keeps data in memory or disk storage across them
    		Each application has its own executors.	
    
    
    	Task	
    		A unit of work that will be sent to one executor	
    
    	Job	
    		A parallel computation consisting of multiple tasks that 
    		gets spawned in response to a Spark action (e.g. save, collect); 
    		you'll see this term used in the driver's logs.
    		一个action对应一个job
    
    	Stage	
    		Each job gets divided into smaller sets of tasks called stages 
    		that depend on each other
    		(similar to the map and reduce stages in MapReduce); 
    		you'll see this term used in the driver's logs.	
    		一个stage的边界往往是从某个地方取数据开始，到shuffle的结束

## Spark Cache
   	rdd.cache(): StorageLevel
   
   	cache它和tranformation: lazy   没有遇到action是不会提交作业到spark上运行的
   
   	如果一个RDD在后续的计算中可能会被使用到，那么建议cache
   
   	cache底层调用的是persist方法，传入的参数是：StorageLevel.MEMORY_ONLY
   	cache=persist
   
   	unpersist: 立即执行的         
   	
## Spark 依赖
    
窄依赖：一个父RDD的partition之多被子RDD的某个partition使用一次	

宽依赖：一个父RDD的partition会被子RDD的partition使用多次，有shuffle
# Spark做过哪些优化（为什么？原理是什么？）

### 1.首先是资源调优 

  从提交参数：
  1.1 executor-cores：每个excutor的最大核数 一般是3到6
  1.2 num-executors：其实是总的excutor数量  就是单个节点的executor数量*节点数。
      (每个节点的executor数量 =单节点yarn的总核数/单个excutor的核数)
	  总的excutor数量 = (单节点yarn的总核数/单个excutor的核数)*节点数
  1.3 executor-memory：单个executor的内存=yarn上配置的单个节点内存/excutor数量	

RDD持久化： rdd持久化可以将数据复用 
            cache() 和persist()   其实cache就是默认的persist() 级别是disk和memory

### 2.CPU优化

 RDD的并行度：一个job一次所能执行的task数目，即一个job对应的总的core资源个数(**spark.default.parallelism**)
             (没有设置时，由 join、reduceByKey 和 parallelize 等转换决定。)
 SparkSql的并行度：默认是200 只能控制Spark sql、DataFrame、DataSet 分区个数
                    不能控制 RDD 分区个数  **spark.sql.shuffle.partitions**
					
 并发度：同时执行的 task 数 有几个core并发度就是多少  

 一般设置并行度是并发度(就是有多少个core)的2到三倍

CPU 低效原因
1）并行度较低、数据分片较大容易导致 CPU 线程挂起
2）并行度过高、数据过于分散会让调度开销更多



### 3.SparkSql语法优化

3.1基于RBO的优化(基于规则的优化器)   主要分为三类 1.谓词下推 2.列裁剪 3.常量替换
3.2基于RBO的优化(基于代价的优化器)
   生成表级别统计信息（扫表）：ANALYZE TABLE 表名 COMPUTE STATISTICS
   生成表级别统计信息（不扫表）：ANALYZE TABLE src COMPUTE STATISTICS NOSCAN
   生成列级别统计信息:ANALYZE TABLE 表名 COMPUTE STATISTICS FOR COLUMNS 列 1,列 2,列 3
DESC FORMATTED 表名

3.3广播join
   通过参数指定自动广播  .set("spark.sql.autoBroadcastJoinThreshold","10m")
   强行广播： select  /*+  BROADCASTJOIN(sc) */   sc是表的别名

3.4 SMB join sort merge bucket 操作,首先会进行排序,然后根据 key值合并，把相同 key 的数据放到同一个 bucket 中（按照 key 进行 hash）。
    使用条件：1.两表进行分桶，桶的个数必须相等  2.两边进行 join 时，join的列=排序的列=分桶的列
	

### 4.数据倾斜

#### 4.1产生原因？

设计shuffle类的算子时，某个key的数量特别大

#### 4.2 数据倾斜大 key 定位

使用sample算子对key不放回采样 计算出现的次数后排序取前十

#### 4.3 单表数据倾斜优化 (*)

两阶段聚合（加盐局部聚合+去盐全局聚合）

#### 4.4 join数据倾斜优化

#####   4.4.1 广播 Join

适用于小表 join 大表。小表足够小，可被加载进 Driver 并通过 Broadcast 方法广播到各个 Executor 中

##### 4.4.2 拆分大 key 打散大表 扩容小表

​	1.首先将大表分为带有倾斜key和不带有倾斜key的

       2. 将倾斜的key打散  打散36份
       3. 小表进行扩容 扩大36倍
       4. 倾斜的大key和扩容后的表进行join
       5. 没有倾斜大key的部分 与 原来的表进行join
       6. 将倾斜key join后的结果与普通key join后的结果 union起来

### 5.job优化

#### 5.1Map端聚合

RDD 的话建议使用 reduceByKey 或者 aggregateByKey 算子来替代掉 groupByKey 算子。因为 reduceByKey 和 aggregateByKey 算子都会使用用户自定义的函数对每个节点本地的相同 key 进行预聚合。而 groupByKey 算子是不会进行预聚合的，全量的数据会在集群的各个节点之间分发和传输，性能相对来说比较差。
SparkSQL 本身的 HashAggregte 就会实现本地预聚合+全局聚合

#### 5.2读取小文件优化

spark.sql.files.maxPartitionBytes=128MB  (设置每个分区最大为128M  默认 128m)
spark.files.openCostInBytes=4194304   （打开文件的开销）默认 4m



totalBytes = （单个文件的大小+openCostInbytes）*文件个数 

 bytesPerCore =totalBytes/defaultParallelism  文件的总大小/默认的并行度
 Math.min(defaultMaxSplitBytes,Math.max(openCostInbytes,bytesPerCore))

当（文件 1 大小+ openCostInBytes）+（文件 2 大小+ openCostInBytes）+…+（文件n-1 大小+ openCostInBytes）+ 文件 n <= maxPartitionBytes 时，n 个文件可以读入同一个分区，即满足： N 个小文件总大小 + （N-1）*openCostInBytes <= maxPartitionBytes 的话

#### 5.3 增大 map 溢写时输出流 buffer

![image-20220504180522834](https://s2.loli.net/2022/05/04/YHaxOvtSgpsq2y4.png)

```
  buffer缓冲区大小是5M(超过会*2) 和缓冲的批次10000条 这两个参数无效   
  溢写时使用输出流缓冲区默认 32k  我们可以修改这个为 .set("spark.shuffle.file.buffer", "64")
```



#### 5.4 Reduce端的优化

##### 5.4.1 合理设置reduce的个数

就是shuffle后的分区数 也就是task的数量 比如sparksql的默认200  并行度是并发度的2到3倍

##### 5.4.2输出产生的小文件优化

###### 一、Join 后的结果插入新表

join 结果插入新表，生成的文件数等于 shuffle 并行度，默认就是 200 份文件插入到hdfs 上。

1.可以在插入表数据前进行缩小分区操作来解决小文件过多问题，如 coalesce、repartition 算子。
2.调整 shuffle 并行度。

RDD的并行度：一个job一次所能执行的task数目，**spark.default.parallelism**
 SparkSql的并行度：默认是200 只能控制Spark sql、DataFrame、DataSet 分区个数，不能控制 RDD 分区个数  ***spark.sql.shuffle.partitions***

###### 二、动态分区插入数据

1.没有 Shuffle 的情况下。最差的情况下，每个 Task 中都有表各个分区的记录，那文件数最终文件数将达到 Task 数量 * 表分区数。这种情况下是极易产生小文件的。
INSERT overwrite table A partition ( aa )
SELECT * FROM B;

2.有 Shuffle 的情况下，上面的 Task 数量 就变成了 spark.sql.shuffle.partitions（默认值200）。那么最差情况就会有 spark.sql.shuffle.partitions * 表分区数。当 spark.sql.shuffle.partitions 设 置 过 大 时 ， 小 文 件 问 题 就 产 生 了 ； 当
spark.sql.shuffle.partitions 设置过小时，任务的并行度就下降了，性能随之受到影响。最理想的情况是根据分区字段进行 shuffle，在上面的 sql 中加上 distribute by aa。把同一分区的记录都哈希到同一个分区中去，由一个 Spark 的 Task 进行写入，这样的话只会产生 N 个文件, 但是这种情况下也容易出现数据倾斜的问题。

解决思路：
结合第 4 章解决倾斜的思路，在确定哪个分区键倾斜的情况下，将倾斜的分区键单独
拎出来：
将入库的 SQL 拆成（where 分区 != 倾斜分区键 ）和 （where 分区 = 倾斜分区键） 几
个部分，非倾斜分区键的部分正常 distribute by 分区字段，倾斜分区键的部分 distribute by
随机数，sql 如下：

//1.非倾斜键部分
INSERT overwrite table A partition ( aa )
SELECT *  FROM B where aa != 大 key
distribute by aa;

//2.倾斜键部分
INSERT overwrite table A partition ( aa )
SELECT * FROM B where aa = 大 key
distribute by cast(rand() * 5 as int);

##### 5.4.3  增大 reduce 缓冲区，减少拉取次数

Spark Shuffle 过程中，shuffle reduce task 的 buffer 缓冲区大小决定了 reduce task 每次能够缓冲的数据量，也就是每次能够拉取的数据量，如果内存资源较为充足，适当增加拉取数据缓冲区的大小，可以减少拉取数据的次数，也就可以减少网络传输的次数，进而提升性能。
reduce 端数据拉取缓冲区的大小可以通过 **spark.reducer.maxSizeInFlight** 参数进行设置，默认为 **48MB**

##### 5.4.4 调节 reduce 端拉取数据重试次数

reduce 端拉取数据重试次数可以通过 **spark.shuffle.io.maxRetries** 参数进行设置，该参数就代表了可以重试的最大次数。如果在指定次数之内拉取还是没有成功，就可能会导致作业执行失败，**默认为 3**

##### 5.4.5 调节 reduce 端拉取数据等待间隔

reduce 端拉取数据等待间隔可以通过 **spark.shuffle.io.retryWait** 参数进行设置，**默认值为 5s**

#### 5.5 合理利用 bypass

```
 .set("spark.shuffle.sort.bypassMergeThreshold", "30") //bypass阈值，默认200,改成30对比效果
```

### 6.整体优化

#### 6.1调节数据本地化等待时长

在 Spark 项目开发阶段，可以使用 client 模式对程序进行测试，此时，可以在本地看到比较全的日志信息，日志信息中有明确的 Task 数据本地化的级别，如果大部分都是**PROCESS_LOCAL(本地)、NODE_LOCAL(节点)**，那么就无需进行调节，但是如果发现很多的级别都是 **RACK_LOCAL(机架)、ANY(不同机架)**，那么需要对本地化的等待时长进行调节，

```
spark.locality.wait          //建议 6s、10s
spark.locality.wait.process  //建议 60s
spark.locality.wait.node     //建议 30s
spark.locality.wait.rack     //建议 20s
```

#### 6.2 使用堆外内存

**spark.executor.memory**：提交任务时指定的堆内内存。

**spark.executor.memoryOverhead：**堆外内存参数(内存额外开销)。默认开启，默认值为 spark.executor.memory*0.1 并且会与最小值 384mb 做对比，取最大值。

所以 spark on yarn 任务堆内内存申请 1 个 g，而实际去 yarn 申请的内存大于 1 个 g 的原因。

**spark.memory.offHeap.size** ： 堆 外 内 存 参 数 ， spark 中 默 认 关 闭 ， 需 要 将spark.memory.enable.offheap.enable 参数设置为 true。



不同版本不同：

```
spark2.4.5    executor.memory+executor.memoryOverhead+pySparkWorkerMemory
spark3.0.0    executor.memory+memory.offHeap.size(堆外内存)+executor.memoryOverhead(内存开销  默认堆内内存*0.1)+pySparkWorkerMemory
```

##### 6.2.1使用堆外缓存

```
// TODO 指定持久化到 堆外内存
result.persist(StorageLevel.OFF_HEAP)
```

#### 6.3 调节连接等待时长

​       在 Spark 作业运行过程中，Executor 优先从自己本地关联的 BlockManager 中获取某份数据，如果本地 BlockManager 没有的话，会通过 TransferService 远程连接其他节点上Executor 的 BlockManager 来获取数据。
​      如果 task 在运行过程中创建大量对象或者创建的对象较大，会占用大量的内存，这回导致频繁的垃圾回收，但是垃圾回收会导致工作现场全部停止，也就是说，垃圾回收一旦执行，Spark 的 Executor 进程就会停止工作，无法提供相应，此时，由于没有响应，无法建立网络连接，会导致网络连接超时。
​     在生产环境下，有时会遇到 file not found、file lost 这类错误，在这种情况下，很有可能是 Executor 的 BlockManager 在拉取数据的时候，无法建立连接，然后超过默认的连接等待时长 120s 后，宣告数据拉取失败，如果反复尝试都拉取不到数据，可能会导致 Spark 作业的崩溃。这种情况也可能会导致 DAGScheduler 反复提交几次 stage，TaskScheduler 反复提交几次 task，大大延长了我们的 Spark 作业的运行时间。为了避免长时间暂停(如 GC)导致的超时，可以考虑调节连接的超时时长，连接等待时长需要在 spark-submit 脚本中进行设置，设置方式可以在提交时指定：

--conf spark.core.connection.ack.wait.timeout=300s



### 7. spark AQE

Spark 在 3.0 版本推出了 AQE（Adaptive Query Execution），即自适应查询执行。**AQE 是Spark SQL 的一种动态优化机制**，在运行时，每当 Shuffle Map 阶段执行完毕，AQE 都会结 合这个阶段的统计信息，基于既定的规则动态地调整、修正尚未执行的逻辑计划和物理计划，来完成对原始查询语句的运行时优化。

#### 7.1 动态合并分区

```
.set("spark.sql.adaptive.enabled", "true") //AQE的总开关
.set("spark.sql.adaptive.coalescePartitions.enabled", "true") // 合并分区的开关
.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum","1000") // 初始的并行度
.set("spark.sql.adaptive.coalescePartitions.minPartitionNum","10") // 合并后的最小分区数
.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "20mb") // 合并后的分区，期望有多大

.set("spark.dynamicAllocation.enabled","true")  // 动态申请资源  默认false   这样就能申请更多的excutor 并发度得到提升
//.set("spark.dynamicAllocation.shuffleTracking.enabled","true") // shuffle动态跟踪
```



#### 7.2动态切换Join策略

比如有A表和B表 A表4.2G   B表2个G  筛选条件后B表为10M  如果没有动态切换join策略开关 就走普通的sortshuffle

如果开启后 就走广播了

```
.set("spark.sql.adaptive.enabled", "true")  //AQE的总开关
.set("spark.sql.adaptive.localShuffleReader.enabled", "true") //在不需要进行shuffle重分区时，尝试使用本地shuffle读取器。将sort-meger join 转换为广播join
```



#### 7.3 动态优化Join倾斜

```
1）spark.sql.adaptive.skewJoin.enabled :是否开启倾斜 join 检测，如果开启了，那么会将倾斜的分区数据拆成多个分区,默认是开启的，但是得打开 aqe。 

2）spark.sql.adaptive.skewJoin.skewedPartitionFactor :默认值 5，此参数用来判断分区数 据量是否数据倾斜，当任务中最大数据量分区对应的数据量大于的分区中位数乘以此参数，并且也大于 spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 参数，那么此任务是数据倾斜。

3）spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes :默认值 256mb，用于判断是否数据倾斜
4）spark.sql.adaptive.advisoryPartitionSizeInBytes :此参数用来告诉 spark 进行拆分后推荐分区大小是多少

.set("spark.sql.adaptive.enabled", "true")// 开启AQE的总开关

判断条件：
条件1：skewedPartitionFactor(默认是5) * 当前分区大小> 所有分区大小的中值（比如有200个分区 这里指的是第100个分区的大小）
条件2：skewedPartitionFactor(默认是5) * 当前分区大小> skewedPartitionThresholdInBytes
当条件1和条件2都满足时，认为当前分区是倾斜的。

需要注意的是和合并分区(spark.sql.adaptive.coalescePartitions.enabled)有冲突会影响对分区大小的判断  
如果同时开启会先合并分区
```

### 8.DPP(动态裁剪 谓词下推的根)

Spark3.0 支持动态分区裁剪 Dynamic Partition Pruning，简称 DPP，核心思路就是先将join 一侧作为子查询计算出来，再将其所有分区用到 join 另一侧作为表过滤条件，从而实现对分区的动态修剪。如下图所示:

![image-20220506195226809](https://s2.loli.net/2022/05/06/IkZGaD38VHKUJME.png)

```
将 select t1.id,t2.pkey from t1 join t2 on t1.pkey =t2.pkey and t2.id<2 优化成了
select t1.id,t2.pkey from t1 join t2 on t1.pkey=t2.pkey and t1.pkey in(select t2.pkey from t2 where t2.id<2)

触发条件：
（1）待裁剪的表 join 的时候，join 条件里必须有分区字段
（2）如果是需要修剪左表，那么 join 必须是 inner join ,left semi join 或 right join,反之亦然。但如果是 left out join,无论右边有没有这个分区，左边的值都存在，就不需要被裁剪
（3）另一张表需要存在至少一个过滤条件，比如 a join b on a.key=b.key and a.id<2
参数 spark.sql.optimizer.dynamicPartitionPruning.enabled 默认开启。
```

### 9 .Hint增强

#### 9.1 broadcasthast join

```
sparkSession.sql("select /*+ BROADCAST(school) */ * from test_student student left join test_school school on student.id=school.id").show()

sparkSession.sql("select /*+ BROADCASTJOIN(school) */ * from  test_student student left join test_school school on student.id=school.id").show()

sparkSession.sql("select /*+ MAPJOIN(school) */ * from test_student student left join test_school school on student.id=school.id").show()
```

#### 9.2  sort merge join

```
sparkSession.sql("select /*+ SHUFFLE_MERGE(school) */ * from test_student student left join test_school school on 
student.id=school.id").show()

sparkSession.sql("select /*+ MERGEJOIN(school) */ * from test_student student left join test_school school on student.id=school.id").show()

sparkSession.sql("select /*+ MERGE(school) */ * from test_student student left join test_school school on student.id=school.id").show()
```

#### 9.3 shuffle_hash join

```
sparkSession.sql("select /*+ SHUFFLE_HASH(school) */ * from test_student student left join test_school school on student.id=school.id").show()
```


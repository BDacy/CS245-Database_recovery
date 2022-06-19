## 数据库故障恢复
> 此次项目实验主要来自斯坦福CS245
> 项目代码仓库链接为:https://github.com/stanford-futuredata/cs245-as3-public

### 要做什么
整体工作需要实现一个支持持久化的事务管理系统。
### 怎么做
#### 第一步
需要在“TransactionManager.java”中实现如下接口：
* start(long txID)
    - 为开启一个新的事务，接口需要确保分配的txID单调递增（即使在系统crash后，仍需满足该特性）。
* read(long txID, long key)
    - 返回任意事务最近提交的key的value。
* write(long txID, long key, byte[] value)
    - 将数据写入数据库，需保证在该事务提交前，数据不能被read()接口读到。为了简化场景，不考虑同一个事务内写了一个key，在读取相同key的情况。
* commit(long txID)
    - 提交一个事务，使该事务写入的数据被后续的read()操作读取。
* abort(long txID)
    - 回滚一个事务，使该事务和没发生一样。    
* writePersisted(long key, long tag, byte[] value)
    - 暂时不用管，这个会在第二部分详细描述。

然后，需要为TransactionManager实现一个初始化和恢复API：
* initAndRecover(LogManager lm, StorageManager sm)
    - 使用log manager 和 storage manager 初始化 transaction manager，即初始化自身的核心数据结构。在测试用例中，将使用LogManager和StorageManager实例调用该接口。当然，测试中会保证该方法最先被调用。

上面主要实现了一个事务的基本接口，用以保证事务的原子性（Atomic），其目的是：在没有故障发生的情况下，能保证一定能读到最后提交的事务修改的数据。
除了原子性，项目还需要考察持久性。

持久性通过调用LogManager和StorageManager类的方法来实现。

LogManager和StorageManager类能从不同角度保证持久性，注意log只能通过追加（append）写日志记录（log records）。

Log records 是一个学生自定义格式（format）的有大小限制的char 型数组。并且追加的记录也具有原子性，即如果在追加之后仍然有该记录的追加动作，这个动作会被阻塞（即不会再追加同样的记录）。

Storage Manager 采用异步持久化更新数据，注意：在写相同key时需按序进行持久化，写不同key时可乱序持久化。类及API如下：
- LogManager
    - appendLogRecord(byte[] record)
        - 原子追加和持久化日志记录到log尾部，确保前面的追加写已经成功后才执行本次写入。
    - getLogEndOffset()
        - 返回日志在最后一次写入后的偏移
    - readLogRecord(int position, int size)
        - 返回偏移为position/长度为size的日志段。超过range之后返回异常。
    - setLogTruncationOffset(int offset)
        - 从持久化存储中删除偏移为offset之前的日志，将合法的起始点设置为offset。
    - getLogTruncationOffset()
        - 返回当前truncation 的偏移。
- StorageManager
    - queueWrite(long key, long tag, byte[] value)
        - 写不同key时，可能发生持久化顺序和调用queueWrite接口不一致的情况；写相同key时，持久化顺序保证和调用顺序一致。你可以认为和每个key有一个独立的队列一样。
    - Map<Long, TaggedValue> readStoredTable()
    - 返回系统最后一次故障前的key和value的mapping。首次初始化的数据库返回空map。
      

需要思考如何使用LogManager和StorageManager的接口来存储写入的数据。需要设计一个序列化格式来通过log records来持久化写入的数据。
推荐在事务提交前将待写入的数据缓存在内存中，仅在提交时将数据写入LogManager和StorageManager中。推荐使用redo logging的方式。

#### 第二部分：Truncating the Log and Fast Recovery

由于如果每次恢复数据库都需要从头开始应用（或者从尾应用）日志的话太费时费力，所以这个部分的任务是需要为了降低恢复成本来截断数据。

当queue的写入被持久化的时候，StorageManager会调用writePersisted。

需要实现一个方案来追踪哪些提交的写入没有被StorageManager应用并持久化。

从这些信息中，需要得出一个日志截断点，该截断点不会超过尚未持久化到日志中的任何写入点。
并且需要定期调用setLogTruncationPoint来截断日志。并且当出现故障的时候，从最后一个日志截断点开始恢复数据库。

本部分的考核重点，也是最需要注意的一点，是在数据恢复时尽量减少日志的阅读次数。recoveryPerformance 测试用于检查日志在操作期间是否被截断，以及恢复数据库时对日志进行的少量I/O。

### 解决方案
#### 日志格式
对日志的格式进行规定如下

![日志格式](https://github.com/BDacy/CS245-Database_recovery/edit/main/Record结构图.png)
对各个字段的解释如下：
len:该条record日志的长度
txID:事务ID
type:该条日志的类型，0-write;1-commit
key:某个变量
value:key的更新值

日志有两种类型，写类型和提交类型，对应record中type的取值0和1;
写类型日志如上图
而提交类型日志需要把上述Record日志结构去掉key和value的部分

#### 数据库恢复方案

数据库恢复方案基于先前给出的结构，使用redo的方案
1. 即先扫描一遍日志，获取已提交事务的事务id放入txCommit队列或集合中;
2. 再从头到尾扫描日志，对于遇到的每一条日志
    - 如果该条日志是写操作且事务id在txCommit中，属于已提交的事务，则写入存储器中。
    - 如果其事务id不属于已提交事务，则什么都不做。
    - 如果该条日志不是写操作，也什么都不做

注：在此解决方案中，日志的类型只有写或者提交，不存在其他类型的日志

#### 截断点的选取
> 如果每次故障恢复都要从日志的头读取恢复，那要做的恢复工作也太多了
> 于是就需要进行一个检查点checkpoint的设置

如何找到一个检查点？

在此次项目中，当StorageManager(以下简称sm)调用do_persistence_work方法时，会将sm中key的latest_persion写到persisted_persion中(crash时latest_persion会置null,而persisted_persion不会)，来模拟数据库写入存储器的操作。
此时会调用TransactionManager(简称tm)的writePersisted方法。

在writePersisted方法中我们维护一个offsetSet来存储已经在sm中persisted持久化的偏移量persisted_tag(对应record日志的尾部偏移量),与我们每次在queueWrite后写入的persisted日志中的偏移量进行比较。

如果说persisted日志中的最小值在offsetSet已经存在，表明该偏移量对应的日志操作已经真正的被持久化了，可以在这个偏移量上设置检查点(截断点)。

详细实现请看writePersisted方法

#### TransactionManager的实现

#####  用到的数据结构
```java
private LogManager lm;
private StorageManager sm;
//	记录当前的最大事务id，以确保下个start事务的合法性
private long MaxTxID;
// 	在writePersisted方法中使用
private HashSet<Integer> OffsetSet;
// 	存放已经进行持久化的日志record在lm中的偏移量
private PriorityQueue<Integer> persisted;
// 	key-txID,value-record日志集合
private Map<Long,ArrayList<Record>> RecordMap;
```
各个方法的实现见TransactionManager.java
有比较详细的注释

#### 关于测试

TransactionManagerTests.java是测试类用来测试ACID特性

理论上应该可以全部通过测试

注意：TransactionManagerTests.java中的TestRepeatedFailures()测试方法会涉及到多测crash的测试，同时会限制读取或写日志的次数。
同时，某些测试还有一定的io要求，实现方法时请注意。


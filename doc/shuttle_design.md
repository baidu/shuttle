shuttle框架设计
====

## 设计背景
Galaxy是一个集群调度和管理系统，可以将用户程序部署在集群上运行。同时Galaxy仍需要一个基于Map Reduce框架的流式计算模型，以便用户可以在Galaxy上直接进行Map Reduce框架的批处理任务而不需要额外的配置。基于此目的我们设计了shuttle。

## 需求分析
1. 基于Galaxy
该计算框架基于Galaxy调度系统，依靠Galaxy集群进行资源分配和任务管理。
2. 松耦合于现有分布式文件系统
计算框架的输入输出应当支持从现有的分布式文件系统，如HDFS上，读取写入数据。

## 设计思路
设计整体思路如下图。  
1. 主从式结构部署  
shuttle利用一个全局唯一的master来接受用户提交的请求，并利用Galaxy提供的sdk开启minion来控制管理用户的Map和Reduce批处理任务。minion负责管理批处理任务的输入和输出，并将运行状况汇报给master。  
2. 利用HDFS缓存中间结果  
shuttle负责将Map的输出排序后发送给Reduce。为了支持大规模数据的处理，因此我们采取将Map的中间输出暂存至HDFS的方法，来应付大规模的数据，减少对内存的压力，以在Galaxy上获取更多的槽位。  
3. 包管理  
minion除负责提供输入处理输出外还具有包管理的功能。用户使用客户端提交的运行所需要的文件不是一开始打包给Galaxy，而是暂存在HDFS上，当minion启动运行批处理任务前，会将这些文件从HDFS获取至本地。如编写wordcount程序，所使用的脚本和待处理的文件就暂存在HDFS上。  

    +--------+                   +--------+                 +-------------+
    | master | -- submit job --> | Galaxy | -- schedule --> | minion(Map) | -+
    +--------+                   +--------+                 +-------------+  |             +------+
         |                            |                                      +- sorted --> | HDFS |
         |            +-------+       |                                                    +------+
         +-- meta --> | nexus |       |                +----------------+                      |
                      +-------+       +-- schedule --> | minion(Reduce) | <----- data ---------+
                                                       +----------------+

## 示例 - wordcount
1. 首先客户端接受用户请求，并将批处理文件以及待统计的文件发送至HDFS，生成的地址及命令参数发送给master。
2. master接受用户请求后，根据用户的环境参数调用Galaxy sdk启动minion进行Map任务，同时根据用户指定的HDFS地址找到输入文件，并创建输入分配管理表用于管理输入分配给各个Map的情况。
3. Map从master处获取id等信息，并从HDFS上获取输入数据以及Map启动所需的文件，运行Map并为其提供输入。将Map的输出暂存、排序后保存在HDFS上。
4. Map运行几近结束时，master可对同一输入开启多个Map的副本来避免长尾效应。多个Map会将输出暂存为HDFS上的临时文件，直到运行结束便重命名为正式输出并做好完成标记。其他副本检查到完成标记后退出。
5. Map运行几近结束时，master便调用Galaxy sdk启动minion进行Reduce任务。Reduce任务会先从HDFS上获取自己的输入并在本地进行归并。
6. Map运行完成后，minion会通知master并重新获取输入，并根据输入启动Map任务。若此时master已将任务分配完毕，则minion正常退出。
7. 运行Reduce的Minion准备好输入后，从HDFS上获取Reduce启动所需的文件，运行Reduce并为其提供输入。将输出保存在HDFS上。完成计算。
8. 若其中运行Map的Minion因为某些原因失败，Galaxy会将其就地拉起。master重新为其分配输入。master从Galaxy处获取minion运行的状况。若minion运行失败则将已分配给该minion的输入数据标记为未分配等待重新分配。
9. 若运行Reduce的Minion因为某些原因失败，Galaxy会将其就地拉起。不需要master干涉重新从HDFS上获取数据。
10. 若用户提供的脚本出现致命错误或返回失败，minion会通知master并将错误记录下来。


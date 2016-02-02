master - 计算任务的接收、管理与分配
====

## 基本框架
    +---------------+
    |  MasterImpl   |
    +---------------+     +-----------------+
    |  JobTracker   | <-- |   DagScheduler  |
    +---------------+     +-----------------+
    |      Gru      | <-- | ResourceManager |
    +---------------+     +-----------------+
    | GalaxyHandler |
    +---------------+

* 以上为生成master中使用的主要的类的层次关系。  
* 每个Minion所需要的任务事实上是一个(`job_id`, `phase_id`, `no`, `attempt`, `file`)组成的五元组(对于某些任务不存在`file`数据)。  
* 因此以上的层次结构中前两层起到类似交换机的功能，将(`no`, `attempt`, `file`)的信息交给`ResourceManager`管理，`Gru`负责协调状态以及提供各种逻辑。  

## 模块功能描述
1. `MasterImpl`: 提供接口，进行job级别的交换  
`MasterImpl`实现了proto文件中声明的master所具有的接口，提供基本的功能。其以job的ID为键存储用于管理每个job的单元`JobTracker`。  
2. `JobTracker`: 管理job的元数据，进行phase级别的交换  
`JobTracker`向`MasterImpl`提供了用于接口实现的功能和数据，并且利用`DagScheduler`管理DAG类型任务中的任务序关系。其按编号存储用于管理每个phase的单元`Gru`。  
3. `Gru`: 管理phase的元数据和各种内部状态，分配并管理一系列任务块  
`Gru`是最重要的数据与功能模块，具体实现`JobTracker`所要求的功能，利用`ResourceManager`来管理任务要求的资源（编号、文件信息）。其通过`GalaxyHandler`向Galaxy递交任务。  
4. `GalaxyHandler`: 与`Galaxy`连接的模块  
`GalaxyHandler`负责向Galaxy提交任务，以及对提交的任务进行更新、关闭等操作。  
5. `DagScheduler`: 管理DAG的序关系  
`DagScheduler`使用邻接表的方式存储提交任务时提供的序关系，方便`JobTracker`调度合适的`Gru`运行。  
6. `ResourceManager`: 管理任务运行的资源  
`ResourceManager`负责管理编号、文件信息等任务执行所需要的信息，同时对任务状态进行记录和管理，在`Gru`中为分配、完成和重分配资源的功能提供支持。  

## 数据流
1. 常规流程  
    1. 提交任务: `MasterImpl`创建`JobTracker`并调用`Start`。`JobTracker`创建`Gru`并调用`Start`。运行信息逐层返回`MasterImpl`作为RPC的返回值。  
    2. 任务申请与完成: 申请或完成信息经由`MasterImpl`, `JobTracker`交给`Gru`。`Gru`从`ResourceManager`中获取信息后逐层返回`MasterImpl`后由RPC返回。  
2. 备份与回复  
    1. 备份: 定时备份，每次备份依次向下调用`Dump`接口获取序列化的数据。最后由`MasterImpl`统一写入iNexus持久化。  
    2. 恢复: 重新启动后恢复时，从iNexus提取信息，按照提交任务的顺序，只是不调用`Start`而是调用`Load`将信息恢复到模块中。  


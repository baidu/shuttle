[shuttle - Galaxy上的Map Reduce计算框架](https://github.com/baidu/shuttle)
====

Copyright 2015, Baidu, Inc.

## 概述
shuttle是一个基于[Galaxy分布式集群管理系统](https://github.com/baidu/galaxy)的Map Reduce计算框架。其底层采用分布式文件系统保存中间数据，具有较好的稳定性和速度。

shuttle提供类似Hadoop的用户界面，能够方便的递交不同种类的任务及监视其状态。

shuttle目前仍在不断完善和发展，致力于提供更好的用户体验、更丰富的配置与功能以及更快的运算速度。

## 系统架构
shuttle与经典的Map Reduce架构类似，其利用Galaxy进行资源管理及任务调度，并从HDFS上获取输入并将输出存储在HDFS上，中间数据保存于HDFS或NFS上。

shuttle由client/sdk、Master、Minion三者组成。Master将地址写入[iNexus](https://github.com/baidu/ins)，方便client/sdk以及Minion进行寻址和通信。
* Master负责接受并处理client/sdk发来的请求，管理元数据，利用Galaxy部署Minion并划分输入，并接受Minion的请求分配和管理运算的任务。
* Minion从Master获取任务，进行包管理并准备好用户程序运行的环境，处理输入并递交给用户程序，并获取用户程序的输出进行处理，将用户程序的状态回报Master。
* client/sdk和用户进行交互，获取用户任务的各项信息并提交给Master，也可以从Master处获得任务的数据反馈给用户。

![架构图](https://github.com/baidu/shuttle/blob/master/doc/shuttle.png?raw=true)

## 系统依赖
* 使用[Galaxy](https://github.com/baidu/galaxy)进行资源管理与任务调度
* 使用[iNexus](https://github.com/baidu/ins)进行寻址(同时被Galaxy依赖)
* 使用[sofa-pbrpc](https://github.com/baidu/sofa-pbrpc)进行通信
* 使用分布式文件系统(HDFS/NFS)进行输入输出及中间数据的存储

## 系统构建
** 正在构建 **

目前shuttle暂无Makefile进行构建。我们会尽快完善。


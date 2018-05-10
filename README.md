# UserBehaviorAnalyasis
# UserBehaviorAnalyasis

##项目介绍##

本想基于Apache Spark™,可以进行实时的数据分析.数据源是一些json文件,结果储存在Hbase数据库.

##项目运行环境##

- jdk1.8最新版本
- scala 2.11
- spark 2.3.0
- maven 3以上的版本
- kafka 1.0以上
- hadoop 2.7
- hbase 2.0
- 装有flask模块的python 3.5

##运行项目##

1. 运行python脚本`user_behavior_data.py`在指定目录产生移动互联网行为数据
2. 将项目代码打包成jar包,用`commit_sparkTask.sh`脚本提交

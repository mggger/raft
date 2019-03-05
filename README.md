# 关于
[raft算法中文翻译](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)

[zatt raft的python实现](https://github.com/simonacca/zatt)

学习raft算法， 借鉴zatt, 一步一步实现raft


## 实现分为4部分:
1. 选举, 服务之间的通信， 各种身份切换的逻辑,  实现对应分支(dev1)    

2. 集群成员变化，配置更新, 实现对应分支(dev2)

3. 日志压缩, 安装快照, 实现对应分支(dev3)

4. 分布式kv(master)

### 进度
- [x]  [目标1](https://github.com/mggger/raft/tree/dev1) | [笔记](https://mggger.github.io/2019/03/raft%E7%AC%94%E8%AE%B0-1/)
- [x]  [目标2](https://github.com/mggger/raft/tree/dev2) | 笔记
先实现Server不主动返回LeaderId,不删除已经收到回复的Reply
但是要避免执行已经执行过的请求

### 问题记录
> Leader L1 收到命令后重新选举出新的Leader L2，此时客户端还在等待，但 L2 没有客户端想要的命令，并且由于 L2 没有客户端请求，就一直没有读取提交的日志，导致整个系统卡住
>
> 解决：接收已提交日志时检测到**任期发生**改变应该返回 ErrWrongLeader，然后应该开启协程单独用来接收消息？

> 请求只能由 Leader 回复吗? 还是说 Follower 也可以回复

### 可参考优化
mit 6.824-2021-lab3 总体认识和实验记录 - 晰烟的文章 - 知乎
https://zhuanlan.zhihu.com/p/576109272
何时迁移?

> 确保副本组中的所有服务器按照它们执行的操作顺序在同一点进行迁移
>
> Raft日志接收到config后改变Shard状态为Pulling, 协程中检测到Pulling就去请求,请求到后改为CGing开始提供服务,

谁来迁移?

> **完全由 Leader** 还是 每个 Server迁移到另一个组中编号对应的 Server?

新的重复请求检测方式?

> seq不再连续
>
> 迁移到新组后,新的组没有重复检测的记录
>
> 将重复检测分组, 并记录每个 cid 最后一个 seq, 发送分片的时候一起发送



重放日志的过程中,检测到新的config,拉去分片并应用后,继续重放日志,就会出错? -> config版本号

之前的问题: 

现在 group1和group2 在config4 分别负责一个 shard, 当 group1 重启后按顺序得到config,当得到config3 时就会向group2请求分片2,但现在 分片2 是 group2 在服务,这样就出现2个group对某个分片同时服务?
config 0: 0 0
config 1: 1 1
config 2: 1 2
config 3: 1 1
config 4: 1 2

当一个group重启后,从config.num=1开始获取config,获取到config后放入Raft,此时不用担心Server会根据获取到的前一个config和当前获取到的config开始去请求这个过时config中属于自己的Shard从而导致同一时刻有两个group服务同一个Shard(因为这个Config还未应用,只是放入了Raft),因为重启后会回放日志,日志中有重启之前这个Server经历过的config,当看到重启后重新获取到的过时config时会因为config.num将这些过时config直接丢弃(所以这些config根本没机会去Pull Shard)

S=重启的Server

为什么用 < ?
因为在S下线这段时间,其它的Server可能已经成功经历了几次config变化(不需要这个S参与 -> 不需要从S Pull Shard, 所以他们可以在不需要这个S的情况下推进Config),所以S的config可能 < 其它Server

当S重启后获取到它下线前config(C0)的下一版本的config(C1)后,它可以比较这两个config差异然后去Pull它需要的Shard,这时不用担心有其它Server正在服务这个Shard,因为这个Shard在这个C1属于S, 那么如果其它Server的config中也服务这个Shard,那么它们是需要等待这个S, 其它Server只是得到了Config,但不能真正开始服务这个Shard,因为在服务之前需要先从S Pull这个Shard, 但S会因为 config.num < args.num 拒绝其它Server的 Pull请求 即 (需要等待S服务完 -> S的config追上它们).
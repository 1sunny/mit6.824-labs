> dialing:dial unix /var/tmp/824-mr-501: connect: connection refused 

这个报错出现的原因是worker调用RPC时master已经退出


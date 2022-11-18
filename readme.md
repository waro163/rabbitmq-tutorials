# 概述
一个rabbitmq的go项目demo

# download dependency
```
go mod tidy
go mod vendor
```

# run worker
```
go run -mod=vendor topic/worker.go --qs=info_queue,warn_queue --rk=test
```

# run producer
```
go run -mod=vendor topic/publish.go
```

# 注意

生产消息时，输入的routing key 需要与worker中的rk参数一致才能发送到对应的queue中
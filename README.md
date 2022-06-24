# STUPID-KV
A simple KV database implemented in Golang, course project for THU2022 Spring (Big Data)

Supported Features
+ Put/Get/Inc/Dec/Del operations
+ Concurrency support using `sync.Map`
+ Persistent to disk (json format)
+ Transaction supported using 2PL protocol
  + begin/commit/abort

TODOS
+ MVCC protocol
+ Undo log has not been persistent yet
+ ...
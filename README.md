# STUPID-KV
A simple KV database implemented in Golang, course project for THU2022 Spring (Big Data)

Supported Features
+ Put/Get/Inc/Dec/Del operations
+ Concurrency support using `sync.Map` as storage layer
+ Persistent to disk (json format)
+ Transaction supported using 2PL protocol (2pl branch)
  + begin/commit/abort
+ MVCC protocol
  + MV2PL referencing 
    + An Empirical Evaluation of In-Memory Multi-Version Concurrency Control
    + https://15721.courses.cs.cmu.edu/spring2019/slides/03-mvcc1.pdf
  + not fully tested yet
  + gc not implemented

TODOS
+ Undo log has not been persistent yet
+ Vulnerable to incident shutdown
+ Interactive query
+ Distributed
+ ...
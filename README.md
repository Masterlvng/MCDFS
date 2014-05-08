MCDFS -- Simple Distributed File System based on raft
=====

## Overview
MCDFS is inspired by [Heystack](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf).
Its stand-alone storage is the simplified [weed-fs](https://code.google.com/p/weed-fs/), so it has better performance.
[raft](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) is used as MCDFS's consensus Algorithm


```
# start two processes
$ MCDFS -vl YOUR_VOLUME_LOCATION /tmp/node.1
$ MCDFS -join localhost:4001 -vl YOUR_ANOTHER_LOCATION /tmp/node.2

$ curl -F "file=@sample.jpg;type=image/jpg" http://127.0.0.1:4001/write
  1

  {'Cookie':13412123,'Offset':'0','Size':1234123}
$ wget http://127.0.0.1:4002/read/1/0/1234123/13412123
  (get file just uploaded)
```

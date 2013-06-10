#System Overview:

This project is designed to integrate MongoDB with relational database. This will alllow all the documents
in mongoDB to be stored in RDBMS systems and both system will be in sync while project is running.
It is based on  [Storm](https://github.com/nathanmarz/storm/ "Storm")  real-time stream processing framework from Twitter.

#System Internals:
The MongoOpLogSpout connects to either a mongod or a mongos, depending on cluster setup, and spawns an MongoSpoutTask
thread for every primary node in the cluster.
These MongoSpoutTask thread continuously poll the Oplog for new operations,
get the relevant documents, put them into  LinkedBlockingQueue data structure,
where MongoOpLogSpout pick them up and emit to next elements in the topology.

When topology starts, tailable cursor to the Oplog of the mongod is created.
This cursor will return all new entries, which are processed by next element in topology.
If this is the first time the topology is being run, then a special "dump collection" occurs where all the documents
from a given namespace are dumped into the target system.
On subsequent calls, the timestamp of the last oplog entry is stored in the system,
so the OplogThread resumes where it left off instead of rereading the whole oplog.


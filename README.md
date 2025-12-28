# RAFT Consensus Algorithm Implementation

Triển khai thuật toán đồng thuận RAFT sử dụng Python và gRPC với đầy đủ các tính năng:

-   Leader election (Bầu chọn leader)
-   Log replication (Đồng bộ log)
-   Fault tolerance (Xử lý lỗi)
-   Network partition handling (Xử lý phân mảnh mạng)
-   Persistent key-value storage (Lưu trữ key-value bền vững)

## Kiến trúc

### Thành phần chính

1. **RaftNode**: Core logic của RAFT node

    - State management (Follower, Candidate, Leader)
    - Election timeout và heartbeat
    - Vote handling

2. **LogManager**: Quản lý log entries

    - Append entries
    - Log compaction
    - Persistence

3. **StateMachine**: Key-value store

    - Apply committed entries
    - GET/SET/DELETE operations

4. **Server/Client**: gRPC communication
    - Inter-node RPC calls
    - Request/Response handling

### Lệnh chạy

#### Compile file

python -m src.server

#### Run node in separate terminals (change ptyxis to your terminal)

for i in {1..5}; do \
ptyxis -e bash -lc "conda activate image_manager && python main.py --node-id node$i --port $((50050+i)); exec bash" & \
done

### Cấu trúc thư mục

````
raft-implementation/
├── main.py ← Start 1 node
├── scripts/
│ ├── start_cluster.py ← Start ALL nodes ⭐
│ ├── stop_cluster.py ← Stop ALL nodes
│ └── monitor.py ← Monitor & test ⭐
├── src/
│ ├── server.py ← gRPC server (auto compile proto)
│ ├── client.py ← gRPC client
│ ├── raft_node.py ← RAFT core logic
│ ├── log_manager.py ← Log management
│ └── state_machine.py ← Key-value store
├── proto/
│ └── raft.proto ← gRPC definitions
├── config/
│ └── cluster_config.yaml ← Configuration
├── data/ ← Persistent storage (auto-created)
└── logs/ ← Log files (auto-created)```
````

### Quy trình khởi động và hoạt động của cluster RAFT

```
USER ACTION
└── python scripts/start_cluster.py

2. START_CLUSTER.PY
   ├── Load config/cluster_config.yaml
   ├── Check ports available
   ├── Create data/ and logs/ directories
   └── For each node:
   └── subprocess: python main.py --node-id nodeX --port 5005X

3. MAIN.PY (cho mỗi node)
   ├── Parse arguments (--node-id, --port)
   ├── Setup logging → logs/nodeX.log
   ├── Create data/nodeX/ directory
   ├── Create RaftNode(nodeX)
   │ ├── Load LogManager (data/nodeX/raft_log.json)
   │ ├── Load StateMachine (data/nodeX/kv_store.json)
   │ ├── Setup RaftClientPool (connect to peers)
   │ └── Initialize RAFT state (term=0, state=FOLLOWER)
   └── Create RaftServer(node)
   └── server.start()

4. SERVER.START()
   ├── ProtoCompiler.prepare()
   │ ├── Clean src/generated/
   │ ├── Compile proto/raft.proto
   │ └── Generate raft_pb2.py, raft_pb2_grpc.py
   ├── Setup gRPC server
   ├── Add RaftServicer (RPC handlers)
   ├── Bind port
   ├── Start serving
   └── node.start()

5. NODE.START()
   ├── Start election_timer_loop (thread)
   │ └── Check election timeout
   │ └── If timeout → start_election()
   └── Wait for events...

6. ELECTION PROCESS
   ├── Follower: election_timeout expired
   ├── → Become CANDIDATE
   ├── → current_term++
   ├── → Send RequestVote to all peers
   ├── → Count votes
   └── If majority → Become LEADER
   └── Start heartbeat_loop (thread)
   └── Send AppendEntries to followers

```

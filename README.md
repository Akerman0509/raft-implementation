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

## Lệnh chạy

### Cài môi trường

```
pip install -r requirements.txt
```

### Compile file .proto

```
python -m compile
```

### Khởi động các node

**1. Linux**

```
for i in {1..5}; do
  gnome-terminal -- bash -c "python main.py --node-id node$i --port $((50050+i)); exec bash" &
done
```

Nếu terminal của bạn là loại khác, hãy thay `gnome-terminal --` bằng lệnh gọi terminal tương ứng.

**2. Windows**:

```
for /L %i in (1,1,5) do start python main.py --node-id node%i --port 5005%i

```

Sử dụng Command Prompt

### Sử dụng file test

**1. Test đồng bộ & ngưỡng chịu đựng**

```
python test_failure.py
```

**2. Test khả năng đồng bộ trong phân mảnh mạng**

```
python test_sync_partition.py
```

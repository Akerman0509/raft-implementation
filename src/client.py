import grpc
import logging
from typing import Dict, List, Optional
import time
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RaftClient:
    """gRPC Client để giao tiếp giữa các RAFT nodes"""
    
    def __init__(self, target_host: str, target_port: int, timeout: int = 5):
        self.target_host = target_host
        self.target_port = target_port
        self.timeout = timeout
        self.address = f'{target_host}:{target_port}'
        self.channel = None
        self.stub = None
        
    def connect(self):
        """Tạo kết nối đến target node"""
        try:
            self.channel = grpc.insecure_channel(
                self.address,
                options=[
                    ('grpc.max_send_message_length', 50 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 50 * 1024 * 1024),
                    ('grpc.keepalive_time_ms', 10000),
                ]
            )
            
            # Import generated stubs
            from src.generated import raft_pb2_grpc
            self.stub = raft_pb2_grpc.RaftServiceStub(self.channel)
            
            # Test connection
            grpc.channel_ready_future(self.channel).result(timeout=self.timeout)
            logger.debug(f"Connected to {self.address}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.address}: {e}")
            return False
    
    def close(self):
        """Đóng kết nối"""
        if self.channel:
            self.channel.close()
            logger.debug(f"Closed connection to {self.address}")
    
    def request_vote(self, term: int, candidate_id: str, 
                     last_log_index: int, last_log_term: int) -> Optional[Dict]:
        """Gửi RequestVote RPC"""
        try:
            from src.generated import raft_pb2
            
            request = raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=candidate_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
            
            response = self.stub.RequestVote(request, timeout=self.timeout)
            
            return {
                'term': response.term,
                'vote_granted': response.vote_granted,
                'voter_id': response.voter_id
            }
            
        except grpc.RpcError as e:
            logger.error(f"RequestVote RPC failed: {e.code()} - {e.details()}")
            return None
        except Exception as e:
            logger.error(f"RequestVote error: {e}")
            return None
    
    def append_entries(self, term: int, leader_id: str, 
                      prev_log_index: int, prev_log_term: int,
                      entries: List, leader_commit: int) -> Optional[Dict]:
        """Gửi AppendEntries RPC"""
        try:
            from src.generated import raft_pb2
            
            # Convert entries to LogEntry messages
            log_entries = []
            for entry in entries:
                log_entries.append(
                    raft_pb2.LogEntry(
                        term=entry['term'],
                        index=entry['index'],
                        command=entry['command'],
                        client_id=entry.get('client_id', '')
                    )
                )
            
            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=leader_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=log_entries,
                leader_commit=leader_commit
            )
            
            response = self.stub.AppendEntries(request, timeout=self.timeout)
            
            return {
                'term': response.term,
                'success': response.success,
                'match_index': response.match_index,
                'follower_id': response.follower_id
            }
            
        except grpc.RpcError as e:
            logger.error(f"AppendEntries RPC failed: {e.code()}")
            return None
        except Exception as e:
            logger.error(f"AppendEntries error: {e}")
            return None
    
    def client_request(self, command: str, client_id: str = "client") -> Optional[Dict]:
        """Gửi client request"""
        try:
            from src.generated import raft_pb2
            
            request = raft_pb2.ClientRequestMsg(
                command=command,
                client_id=client_id
            )
            
            response = self.stub.ClientRequest(request, timeout=self.timeout)
            
            return {
                'success': response.success,
                'result': response.result,
                'leader_id': response.leader_id,
                'error': response.error
            }
            
        except grpc.RpcError as e:
            logger.error(f"ClientRequest RPC failed: {e.code()}")
            return None
        except Exception as e:
            logger.error(f"ClientRequest error: {e}")
            return None
    
    def disconnect_nodes(self, node_ids: List[str]) -> Optional[Dict]:
        """Simulate network partition"""
        try:
            from src.generated import raft_pb2
            
            request = raft_pb2.DisconnectRequest(node_ids=node_ids)
            response = self.stub.DisconnectNodes(request, timeout=self.timeout)
            
            return {
                'success': response.success,
                'message': response.message
            }
            
        except Exception as e:
            logger.error(f"DisconnectNodes error: {e}")
            return None
    
    def heartbeat(self, node_id: str) -> Optional[Dict]:
        """Check node health"""
        try:
            from src.generated import raft_pb2
            
            request = raft_pb2.HeartbeatRequest(node_id=node_id)
            response = self.stub.Heartbeat(request, timeout=self.timeout)
            
            return {
                'alive': response.alive,
                'term': response.term,
                'state': response.state
            }
            
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
            return None
    
    def get_status(self, requesting_node_id: str = "admin") -> Optional[Dict]:
        """Lấy status của node"""
        try:
            from src.generated import raft_pb2
            
            request = raft_pb2.StatusRequest(requesting_node_id=requesting_node_id)
            response = self.stub.GetStatus(request, timeout=self.timeout)
            
            return {
                'node_id': response.node_id,
                'state': response.state,
                'current_term': response.current_term,
                'voted_for': response.voted_for,
                'commit_index': response.commit_index,
                'last_applied': response.last_applied,
                'log_length': response.log_length,
                'cluster_nodes': list(response.cluster_nodes),
                'leader_id': response.leader_id,
                'state_machine': dict(response.state_machine)
            }
            
        except Exception as e:
            logger.error(f"GetStatus error: {e}")
            return None


class RaftClientPool:
    """Quản lý pool các clients đến các nodes trong cluster"""
    
    def __init__(self, cluster_config: Dict):
        self.cluster_config = cluster_config
        self.clients: Dict[str, RaftClient] = {}
        self.disconnected_nodes = set()
    
    def add_node(self, node_id: str, host: str, port: int):
        """Thêm node vào pool"""
        client = RaftClient(host, port)
        self.clients[node_id] = client
        logger.info(f"Added node {node_id} to client pool")
    
    def get_client(self, node_id: str) -> Optional[RaftClient]:
        """Lấy client đến một node"""
        if node_id in self.disconnected_nodes:
            logger.warning(f"Node {node_id} is disconnected")
            return None
        
        client = self.clients.get(node_id)
        if client:
            if not client.channel:
                client.connect()
            return client
        return None
    
    def broadcast_request_vote(self, term: int, candidate_id: str,
                              last_log_index: int, last_log_term: int) -> List[Dict]:
        """Broadcast RequestVote đến tất cả nodes"""
        responses = []
        for node_id, client in self.clients.items():
            if node_id == candidate_id or node_id in self.disconnected_nodes:
                continue
            
            if not client.channel:
                client.connect()
            
            response = client.request_vote(term, candidate_id, last_log_index, last_log_term)
            if response:
                responses.append(response)
        
        return responses
    
    def disconnect_node(self, node_id: str):
        """Simulate disconnect một node"""
        self.disconnected_nodes.add(node_id)
        logger.warning(f"Node {node_id} disconnected")
    
    def reconnect_node(self, node_id: str):
        """Reconnect một node"""
        if node_id in self.disconnected_nodes:
            self.disconnected_nodes.remove(node_id)
            logger.info(f"Node {node_id} reconnected")
    
    def close_all(self):
        """Đóng tất cả connections"""
        for client in self.clients.values():
            client.close()


# Interactive CLI Tool
class RaftCLI:
    """Interactive CLI để test và monitor RAFT cluster"""
    
    def __init__(self, cluster_config: Dict):
        self.pool = RaftClientPool(cluster_config)
        self.setup_cluster(cluster_config)
    
    def setup_cluster(self, config: Dict):
        """Setup client pool từ config"""
        for node in config['cluster']['nodes']:
            self.pool.add_node(node['id'], node['host'], node['port'])
    
    def print_header(self, title: str):
        """In header đẹp"""
        print("\n" + "=" * 70)
        print(f"  {title}")
        print("=" * 70)
    
    def print_node_status(self, status: Dict):
        """In status của một node"""
        if not status:
            print("  ❌ Failed to get status")
            return
        
        state_emoji = {
            'LEADER': '👑',
            'CANDIDATE': '🗳️',
            'FOLLOWER': '👥'
        }
        
        emoji = state_emoji.get(status['state'], '❓')
        
        print(f"\n  {emoji} Node: {status['node_id']}")
        print(f"     State: {status['state']}")
        print(f"     Term: {status['current_term']}")
        print(f"     Leader: {status['leader_id'] or 'None'}")
        print(f"     Voted For: {status['voted_for'] or 'None'}")
        print(f"     Commit Index: {status['commit_index']}")
        print(f"     Log Length: {status['log_length']}")
        
        if status['state_machine']:
            print(f"     Key-Value Store: {len(status['state_machine'])} items")
            for k, v in list(status['state_machine'].items())[:5]:
                print(f"       • {k} = {v}")
    
    def show_cluster_status(self):
        """Hiển thị status của toàn bộ cluster"""
        self.print_header("CLUSTER STATUS")
        
        for node_id in self.pool.clients.keys():
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                self.print_node_status(status)
            else:
                print(f"\n  ❌ Node: {node_id} - DISCONNECTED")
        
        print("\n" + "=" * 70)
    
    def send_command(self, node_id: str, command: str):
        """Gửi command đến một node"""
        self.print_header(f"SEND COMMAND TO {node_id}")
        print(f"  Command: {command}")
        
        client = self.pool.get_client(node_id)
        if not client:
            print(f"  ❌ Cannot connect to {node_id}")
            return
        
        response = client.client_request(command)
        
        if response:
            if response['success']:
                print(f"  ✅ Success!")
                print(f"  Result: {response['result']}")
            else:
                print(f"  ❌ Failed: {response['error']}")
                if response['leader_id']:
                    print(f"  💡 Try leader: {response['leader_id']}")
        else:
            print("  ❌ No response from node")
        
        print("=" * 70)
    
    def simulate_leader_failure(self):
        """Simulate leader failure"""
        self.print_header("SIMULATING LEADER FAILURE")
        
        # Find leader
        leader_id = None
        for node_id in self.pool.clients.keys():
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                if status and status['state'] == 'LEADER':
                    leader_id = status['node_id']
                    break
        
        if leader_id:
            print(f"  👑 Current leader: {leader_id}")
            print(f"  💥 Disconnecting leader...")
            self.pool.disconnect_node(leader_id)
            print(f"  ✅ Leader {leader_id} disconnected")
            print(f"  ⏳ Waiting for new election...")
            time.sleep(3)
            self.show_cluster_status()
        else:
            print("  ❌ No leader found")
        
        print("=" * 70)
    
    def simulate_network_partition(self, group1: List[str], group2: List[str]):
        """Simulate network partition"""
        self.print_header("SIMULATING NETWORK PARTITION")
        
        print(f"  Group 1: {group1}")
        print(f"  Group 2: {group2}")
        
        # Disconnect group1 from group2
        for node_id in group1:
            client = self.pool.get_client(node_id)
            if client:
                client.disconnect_nodes(group2)
        
        # Disconnect group2 from group1
        for node_id in group2:
            client = self.pool.get_client(node_id)
            if client:
                client.disconnect_nodes(group1)
        
        print("  ✅ Network partitioned")
        print("=" * 70)
    
    def run_interactive(self):
        """Chạy interactive mode"""
        self.print_header("RAFT CLUSTER INTERACTIVE CLI")
        
        commands = """
Available commands:
  1. status              - Show cluster status
  2. send <node> <cmd>   - Send command (e.g., send node1 SET key1 value1)
  3. leader-fail         - Simulate leader failure
  4. partition <g1> <g2> - Simulate network partition
  5. reconnect <node>    - Reconnect a node
  6. exit                - Exit CLI
        """
        print(commands)
        
        while True:
            try:
                cmd = input("\n> ").strip()
                
                if not cmd:
                    continue
                
                parts = cmd.split()
                action = parts[0].lower()
                
                if action == 'status':
                    self.show_cluster_status()
                
                elif action == 'send' and len(parts) >= 3:
                    node_id = parts[1]
                    command = ' '.join(parts[2:])
                    self.send_command(node_id, command)
                
                elif action == 'leader-fail':
                    self.simulate_leader_failure()
                
                elif action == 'partition' and len(parts) >= 3:
                    group1 = parts[1].split(',')
                    group2 = parts[2].split(',')
                    self.simulate_network_partition(group1, group2)
                
                elif action == 'reconnect' and len(parts) >= 2:
                    node_id = parts[1]
                    self.pool.reconnect_node(node_id)
                    print(f"  ✅ Node {node_id} reconnected")
                
                elif action == 'exit':
                    print("  👋 Goodbye!")
                    break
                
                else:
                    print("  ❌ Invalid command. Type 'help' for usage.")
            
            except KeyboardInterrupt:
                print("\n  👋 Goodbye!")
                break
            except Exception as e:
                print(f"  ❌ Error: {e}")
        
        self.pool.close_all()


# Example usage
if __name__ == "__main__":
    # Sample cluster config
    cluster_config = {
        'cluster': {
            'nodes': [
                {'id': 'node1', 'host': 'localhost', 'port': 50051},
                {'id': 'node2', 'host': 'localhost', 'port': 50052},
                {'id': 'node3', 'host': 'localhost', 'port': 50053},
                {'id': 'node4', 'host': 'localhost', 'port': 50054},
                {'id': 'node5', 'host': 'localhost', 'port': 50055},
            ]
        }
    }
    
    # Run interactive CLI
    cli = RaftCLI(cluster_config)
    cli.run_interactive()
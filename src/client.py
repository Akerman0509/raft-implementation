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
            logger.info(
                "[Leader send AppendEntries] term=%d leader=%s prev_idx=%d prev_term=%d entries=%d commit=%d",
                request.term,
                request.leader_id,
                request.prev_log_index,
                request.prev_log_term,
                len(request.entries),
                request.leader_commit
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
        if client.connect():
            self.clients[node_id] = client
            logger.info(f"Added node {node_id} to client pool")
            return
        logger.error("Failed to connect to %s", node_id)
        
    def remove_node(self, node_id: str):
        """Xóa node khỏi pool"""
        if node_id in self.clients:
            self.clients[node_id].close()
            del self.clients[node_id]
            logger.info(f"Deleted node {node_id} from client pool")
    
    def get_client(self, node_id: str) -> Optional[RaftClient]:
        client = self.clients.get(node_id, None)
        return client
    
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




# Example usage
if __name__ == "__main__":
    # Sample cluster config
    pass
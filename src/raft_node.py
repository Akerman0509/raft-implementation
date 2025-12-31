"""
RAFT Node Implementation
Core logic cho RAFT consensus algorithm
"""

import time
import random
import logging
import threading
import yaml

from enum import Enum
from typing import Dict, List, Optional
from pathlib import Path

from src.log_manager import LogManager
from src.state_machine import StateMachine
from src.client import RaftClient, RaftClientPool

logger = logging.getLogger(__name__)


class NodeState(Enum):
    """Trạng thái của RAFT node"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class RaftNode:
    """
    RAFT Node implementation
    
    Implements:
    - Leader election
    - Log replication
    - State machine
    - Fault tolerance
    """
    
    def __init__(self, node_id: str, host: str, port: int, 
                 cluster_config: Dict, data_dir: Path):
        # Node identity
        self.node_id = node_id
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
        self.status = "up"
        
        # Cluster configuration
        self.cluster_config = cluster_config
        self.peers = self._build_peer_list(cluster_config, node_id)
        self.data_dir = data_dir
        
        # RAFT state (persistent)
        self.current_term = 0
        self.voted_for: Optional[str] = None
        # voting 
        self.votes_received = 0
        self.majority = 0
        self.votes_from = set()
        
        
        
        # Node state
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        
        # Log management
        self.log_manager = LogManager(data_dir)
        
        # State machine (key-value store)
        self.state_machine = StateMachine(data_dir)
        
        # Volatile state (all nodes)
        self.commit_index = 0
        self.last_applied = 0
        
        # Volatile state (leaders only)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timing
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat_time = time.time()
        self.heartbeat_interval = cluster_config['raft']['heartbeat_interval'] / 1000.0
        
        # Client pool for RPC
        self.client_pool = RaftClientPool(cluster_config)
        self._setup_client_pool()
        
        # Threading
        self.running = False
        self.election_thread: Optional[threading.Thread] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        
        # Disconnected nodes (for testing)
        
        logger.info(f"RAFT Node initialized: {node_id} at {self.address}")
        logger.info(f"Peers: {list(self.peers.keys())}")
    
    def _load_config_from_file(self):
        # Readfile YAML 
        try:
            with open('config/cluster_config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            return self.cluster_config
        
    def _update_node_config(self, config_file='config/cluster_config.yaml'): 
        """Load cluster configuration"""
        try:
            config_path = Path(config_file)
            if not config_path.exists():
                logger.error(f"Config file not found: {config_path}")
                return

            with open(config_path, 'r') as f:
                # Thêm log để xem file path tuyệt đối
                # logger.info(f"Reading config from: {config_path.absolute()}")
                new_config = yaml.safe_load(f)
            
            # debug log
            partitions = new_config.get('cluster', {}).get('partitions', [])
            status = 'unknown'
            if partitions and len(partitions) > 0:
                status = partitions[0].get('status')
            
            # Chỉ log khi status thay đổi hoặc khi đang nghi ngờ lỗi
            # logger.info(f"[CONFIG DEBUG] Partition Status in file: {status}")

            # Update status logic...
            for node in new_config['cluster']['nodes']:
                if node['id'] == self.node_id:
                    self.status = node['status']
                    break
            
            # Update peers
            self.peers = self._build_peer_list(new_config, self.node_id)
            
            # Update client pool
            self._setup_client_pool()
            
        except Exception as e:
            logger.error(f"Error updating config: {e}")
        
        
    def _get_node_partition(self, config: Dict, node_id: str) -> Optional[set]:
        partitions = config["cluster"].get('partitions', [])

        if not partitions or partitions[0].get('status') != 'enabled':
            return None  # không phân mảnh

        for p in partitions[1:]:
            if node_id in p.get('nodes', []):
                return set(p['nodes'])
        
        return set() 
    
    def _build_peer_list(self, config: Dict, node_id: str) -> Dict[str, Dict]:
        peers = {}
        # Xác định partition của node hiện tại
        visible_nodes = self._get_node_partition(config, node_id)
        for node in config['cluster']['nodes']:
            peer_id = node['id']
            if peer_id == node_id:
                continue
            # Nếu partition enabled → chỉ thấy node cùng partition
            if visible_nodes is not None and peer_id not in visible_nodes:
                continue
            peers[peer_id] = {
                'host': node['host'],
                'port': node['port'],
                'address': f"{node['host']}:{node['port']}"
            }
        return peers
    
    def _setup_client_pool(self):
        """Setup client connections to peers"""
        for peer_id, peer_info in self.peers.items():
            if self.client_pool.get_client(peer_id) is None:
                self.client_pool.add_node(peer_id, peer_info['host'], peer_info['port'])
        # delete client in pool in new config
        stale_clients = [
            client_id
            for client_id in self.client_pool.clients.keys()
            if client_id not in self.peers
        ]
        for client_id in stale_clients:
            self.client_pool.remove_node(client_id)
            
    def _update_commit_index(self):
        """
        Kiểm tra xem có thể tăng commit_index không dựa trên match_index của các peers
        """
        # Lấy match_index của chính mình (luôn là log cuối cùng)
        match_indexes = [self.log_manager.get_last_log_index()]
        
        # Lấy match_index của các peers
        for peer_id in self.peers:
            match_indexes.append(self.match_index.get(peer_id, 0))
            
        # Sắp xếp giảm dần để tìm điểm Commit (median)
        match_indexes.sort(reverse=True)
        
        # Commit point là điểm mà quá bán nodes đã lưu
        total_nodes = len(self.peers) + 1
        majority_idx = match_indexes[total_nodes // 2]
        
        if majority_idx > self.commit_index:
            # Chỉ commit log của term hiện tại
            if self.log_manager.get_log_term(majority_idx) == self.current_term:
                self.commit_index = majority_idx
                self._apply_committed_entries()
                logger.info(f"Commit Index updated to {self.commit_index}")     
            
        
    def _random_election_timeout(self) -> float:
        """Generate random election timeout"""
        min_timeout = self.cluster_config['raft']['election_timeout_min'] / 1000.0
        max_timeout = self.cluster_config['raft']['election_timeout_max'] / 1000.0
        return random.uniform(min_timeout, max_timeout)
    
    def start(self):
        """Start RAFT node"""
        logger.info(f"Starting RAFT node {self.node_id}")
        self.running = True
        
        # Start election timer thread
        self.election_thread = threading.Thread(target=self._election_timer_loop, daemon=True)
        self.election_thread.start()
        
        logger.info(f"Node {self.node_id} started as {self.state.value}")
    
    def stop(self):
        """Stop RAFT node"""
        logger.info(f"Stopping RAFT node {self.node_id}")
        self.running = False
        
        if self.election_thread:
            self.election_thread.join(timeout=2)
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
        
        self.client_pool.close_all()
        logger.info(f"Node {self.node_id} stopped")
    
    def _election_timer_loop(self):
        """Election timer loop"""
        while self.running:
            time.sleep(0.05)  # Check every 10ms

            if self.status == 'down':
                time.sleep(1)
                continue

            with self.lock:
                if self.state == NodeState.LEADER:
                    continue
                
                # Check if election timeout expired
                elapsed = time.time() - self.last_heartbeat_time
                if elapsed >= self.election_timeout:
                    logger.info(f"Election timeout! Starting election (elapsed: {elapsed:.2f}s)")
                    self._start_election()

            
    
    def _start_election(self):
        self._update_node_config()
        """Start leader election"""
        # Transition to candidate
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        
        # Reset election timeout
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat_time = time.time()
        
        logger.info(f"Starting election for term {self.current_term}")
        
        # Vote for self
        self.votes_received = 1
        # clean set ()
        self.votes_from.clear()
        self.votes_from = {self.node_id}   # self-vote
        try:
            config = self._load_config_from_file()
            all_nodes_list = config['cluster']['nodes']
            total_nodes = len(all_nodes_list)
        except Exception as e:
            logger.error(f"Error reading config for quorum: {e}")
            # Fallback if error
            total_nodes = len(self.peers) + 1

        majority = (total_nodes // 2) + 1
        self.majority = majority
        
        logger.info(f"Need {majority}/{total_nodes} votes to win")
        
        # Request votes from peers
        last_log_index = self.log_manager.get_last_log_index()
        last_log_term = self.log_manager.get_last_log_term()
        
        for peer_id in self.peers.keys():
            # Send RequestVote in parallel
            threading.Thread(
                target=self._request_vote_from_peer,
                args=(peer_id, last_log_index, last_log_term),
                daemon=True
            ).start()
        
    
    def _request_vote_from_peer(self, peer_id: str, last_log_index: int, last_log_term: int):
        """Request vote from a specific peer"""
        client = self.client_pool.get_client(peer_id)
        if client is None:
            return
        response = client.request_vote(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
        
        if response:
            with self.lock:
                # Check if we're still a candidate
                if self.state != NodeState.CANDIDATE:
                    return
                
                # Check term
                if response['term'] > self.current_term:
                    self._revert_to_follower(response['term'])
                    return
                
                # Count vote
                if response['vote_granted']:
                    if peer_id in self.votes_from:
                        logger.debug(f"Duplicate vote from {peer_id} ignored")
                        return
                    self.votes_from.add(peer_id)
                    self.votes_received += 1
                    logger.info(
                        f"Vote from {peer_id} "
                        f"({self.votes_received}/{self.majority})"
                    )
               
                    if len(self.votes_from) >= self.majority:
                        self._become_leader()
                    
                    
    
    def _become_leader(self):
        """Transition to leader"""
        logger.info(f"Became LEADER for term {self.current_term}")
        
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        
        # Initialize leader state
        last_log_index = self.log_manager.get_last_log_index()
        for peer_id in self.peers.keys():
            self.next_index[peer_id] = last_log_index + 1
            self.match_index[peer_id] = 0
        
        # Start sending heartbeats
        if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self.heartbeat_thread.start()
    
    def _heartbeat_loop(self):
        """Send periodic heartbeats to followers"""
        while self.running and self.state == NodeState.LEADER:
            self._update_node_config()
            self._send_heartbeats()
            time.sleep(self.heartbeat_interval)
    
    def _send_heartbeats(self):
        """Send heartbeat to all followers"""
        if self.status == 'down':
            return
        
        for peer_id in self.peers.keys():
            threading.Thread(
                target=self._send_append_entries_to_peer,
                args=(peer_id,),
                daemon=True
            ).start()
    
    def _send_append_entries_to_peer(self, peer_id: str):
        self._update_node_config()
        # self.status == down 
        
        
        """Send AppendEntries RPC to a peer"""
        client = self.client_pool.get_client(peer_id)
        if not client:
            return
        
        
        # Get log entries to send
        next_idx = self.next_index.get(peer_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = self.log_manager.get_log_term(prev_log_index)
        
        entries = self.log_manager.get_entries_from(next_idx)
        
        response = client.append_entries(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index
        )
        
        if response:
            with self.lock:
                if response['term'] > self.current_term:
                    self._revert_to_follower(response['term'])
                    return
                
                if response['success']:
                    if entries:
                        self.match_index[peer_id] = prev_log_index + len(entries)
                        self.next_index[peer_id] = self.match_index[peer_id] + 1
                        # Update commit index
                        self._update_commit_index()
                else:
                    # Decrement next_index and retry
                    self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

                    
    
    def _revert_to_follower(self, term: int):
        """Revert to follower state"""
        logger.info(f"Reverting to FOLLOWER (term {term})")
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat_time = time.time()
    
    # RPC Handlers
    def handle_request_vote(self, request) -> Dict:
        """Handle RequestVote RPC"""

        self._update_node_config() 
        if self.status == 'down':
            return {'term': self.current_term, 'vote_granted': False}
        
        if request.candidate_id not in self.peers:
            # logger.warning(f"🚫 [FIREWALL] Ignored Vote Request from {request.candidate_id} (Partitioned)")
            return {'term': self.current_term, 'vote_granted': False}
        
        with self.lock:
            response = {
                'term': self.current_term,
                'vote_granted': False
            }
            
            # Check term
            if request.term > self.current_term:
                self._revert_to_follower(request.term)
            
            # Check if we can vote
            can_vote = (
                request.term == self.current_term and
                (self.voted_for is None or self.voted_for == request.candidate_id)
            )
            
            # Check log up-to-date
            last_log_index = self.log_manager.get_last_log_index()
            last_log_term = self.log_manager.get_last_log_term()
            
            log_ok = (
                request.last_log_term > last_log_term or
                (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)
            )
                # test case 1: Split vote scenario => increase term and start again
            # if self.node_id == 'node1' and request.candidate_id == 'node2':
            #     if can_vote and log_ok:
            #         self.voted_for = request.candidate_id
            #         response['vote_granted'] = True
            #         self.last_heartbeat_time = time.time()
            #         logger.info(f"✅ Voted for {request.candidate_id} in term {request.term}")
            # if self.node_id == 'node3' and request.candidate_id == 'node4':
            if can_vote and log_ok:
                self.voted_for = request.candidate_id
                response['vote_granted'] = True
                self.last_heartbeat_time = time.time()
                logger.info(f"Voted for {request.candidate_id} in term {request.term}")
            
            return response
    
    def handle_append_entries(self, request) -> Dict:
        """Handle AppendEntries RPC"""
        
        # 1. Update Config & Check Down
        self._update_node_config() 
        if self.status == 'down':
            return {'term': self.current_term, 'success': False}

        # 2. Firewall Check (Partition)
        if request.leader_id not in self.peers:
            return {'term': self.current_term, 'success': False}
        
        with self.lock:
            response = {
                'term': self.current_term,
                'success': False
            }
            
            # Check term
            if request.term > self.current_term:
                self._revert_to_follower(request.term)
            
            if request.term < self.current_term:
                return response
            
            # Valid leader
            self.last_heartbeat_time = time.time()
            self.leader_id = request.leader_id
            
            if self.state != NodeState.FOLLOWER:
                self.state = NodeState.FOLLOWER
            
            # Check log consistency
            if not self.log_manager.check_log_consistency(request.prev_log_index, request.prev_log_term):
                return response
            
            # Only append if there are entries
            if len(request.entries) > 0:
                entries_to_save = []
                for entry in request.entries:
                    entries_to_save.append({
                        'term': entry.term,
                        'index': entry.index,
                        'command': entry.command,
                        'client_id': getattr(entry, 'client_id', None)
                    })
                
                self.log_manager.append_entries(request.prev_log_index, entries_to_save)
                logger.info(f"📥 [APPEND] Accepted {len(entries_to_save)} entries from {request.leader_id}")
            
            # ------------------------------------
            
            # Update commit index
            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, self.log_manager.get_last_log_index())
                self._apply_committed_entries()
            
            response['success'] = True
            return response
    
    def handle_client_request(self, request) -> Dict:
        """
        Xử lý request từ client (GET/SET)
        Cơ chế: Leader ghi log -> Replicate -> Chờ Quorum -> Commit -> Trả về kết quả
        """
        # 1. Cập nhật config để biết trạng thái mạng hiện tại (Partition/Down)
        self._update_node_config()
        
        if self.status == 'down':
            logger.warning(f"Node {self.node_id} is DOWN. Rejecting client request.")
            return {
                'success': False, 
                'error': 'Node is down',
                'leader_id': None
            }
        
        # 2. Kiểm tra vai trò Leader
        # Chỉ Leader mới được nhận Write request. 
        # (Với Read request, Strong Consistency cũng yêu cầu check Leader)
        with self.lock:
            if self.state != NodeState.LEADER:
                return {
                    'success': False,
                    'error': 'Not leader',
                    'leader_id': self.leader_id
                }
            
            # 3. Parse lệnh
            command = request.command
            parts = command.split()
            if not parts:
                return {'success': False, 'error': 'Empty command'}
            
            operation = parts[0].upper()
            
            # --- XỬ LÝ GET (READ) ---
            if operation == 'GET':
                if len(parts) < 2:
                    return {'success': False, 'error': 'Missing key for GET'}
                key = parts[1]
                value = self.state_machine.get(key)
                return {
                    'success': True,
                    'result': value if value is not None else 'Key not found'
                }

            # --- XỬ LÝ SET/DELETE (WRITE) ---
            # Tạo Log Entry và lưu cục bộ
            entry_index = self.log_manager.get_last_log_index() + 1
            entry = {
                'term': self.current_term,
                'index': entry_index,
                'command': command,
                'client_id': getattr(request, 'client_id', None)
            }
            
            self.log_manager.append_entry(entry)
            logger.info(f"[LEADER] Received command: {command}. Appended to log index {entry_index}")

            # Kích hoạt Replication ngay lập tức (không chờ Heartbeat timer)
            self._send_heartbeats()

        # Chờ Commit (WAIT FOR QUORUM)
        # Leader sẽ chờ cho đến khi entry_index được replicate sang quá bán các node
        # và commit_index được cập nhật.
        
        timeout = 5.0  # Timeout chờ đồng thuận (giây)
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Ngủ ngắn để không chiếm dụng CPU
            time.sleep(0.05) 
            
            with self.lock:
                # Nếu mất chức Leader trong lúc chờ (do partition hoặc node khác lên ngôi)
                if self.state != NodeState.LEADER:
                    return {
                        'success': False, 
                        'error': 'Lost leadership during replication',
                        'leader_id': None
                    }
                
                # KIỂM TRA ĐIỀU KIỆN THÀNH CÔNG:
                # Nếu commit_index đã vượt qua hoặc bằng index của log vừa tạo
                if self.commit_index >= entry_index:
                    logger.info(f"[LEADER] Command {command} committed at index {entry_index}")
                    return {
                        'success': True,
                        'result': 'OK'
                    }

        # Nếu hết timeout mà vẫn chưa commit được (do Partition không đủ Quorum)
        logger.error(f"[LEADER] Timeout waiting for commit index {entry_index}. Possible partition.")
        return {
            'success': False,
            'error': 'Request Timeout (Cluster unstable or Partitioned)',
            'leader_id': self.leader_id
        }
    
    def _apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log_manager.get_entry(self.last_applied)
            
            if entry:
                self.state_machine.apply(entry['command'])
                logger.debug(f"Applied entry {self.last_applied}: {entry['command']}")
    
    def disconnect_nodes(self, node_ids: List[str]):
        """Disconnect from specified nodes (for testing)"""
        self.disconnected_nodes.update(node_ids)
        for node_id in node_ids:
            self.client_pool.disconnect_node(node_id)
        logger.warning(f"Disconnected from: {node_ids}")
    
    def get_status(self) -> Dict:
        """Get current node status"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'commit_index': self.commit_index,
                'last_applied': self.last_applied,
                'log_length': self.log_manager.get_last_log_index(),
                'cluster_nodes': [self.node_id] + list(self.peers.keys()),
                'leader_id': self.leader_id,
                'state_machine': self.state_machine.get_all()
            }
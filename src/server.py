import grpc
from concurrent import futures
import time
import os
import sys
import subprocess
import shutil
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProtoCompiler:
    """Tự động compile proto files trước khi start server"""
    
    def __init__(self, proto_dir='proto', output_dir='src/generated'):
        project_root = Path(__file__).resolve().parent.parent
        self.proto_dir = project_root / proto_dir
        self.output_dir = project_root / output_dir
        self.proto_file = self.proto_dir / 'raft.proto'
    
    def clean_generated_files(self):
        """Xóa các file đã generate trước đó"""
        if self.output_dir.exists():
            logger.info(f"Cleaning generated files in {self.output_dir}")
            shutil.rmtree(self.output_dir)
        
        # Tạo lại thư mục
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Tạo __init__.py
        (self.output_dir / '__init__.py').touch()
        logger.info("Generated directory cleaned and recreated")
    
    def compile_proto(self):
        """Compile proto file thành Python code"""
        if not self.proto_file.exists():
            raise FileNotFoundError(f"Proto file not found: {self.proto_file}")
        
        logger.info(f"Compiling proto file: {self.proto_file}")
        
        # Command để compile proto
        cmd = [
            sys.executable, '-m', 'grpc_tools.protoc',
            f'-I{self.proto_dir}',
            f'--python_out={self.output_dir}',
            f'--grpc_python_out={self.output_dir}',
            str(self.proto_file)
        ]
        
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("Proto compilation successful!")
            if result.stdout:
                logger.debug(f"Output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Proto compilation failed: {e.stderr}")
            raise
        
        # Kiểm tra files đã được tạo
        generated_files = list(self.output_dir.glob('*.py'))
        logger.info(f"Generated files: {[f.name for f in generated_files]}")
        
        return True
    
    def fix_imports(self):
        """Fix relative imports trong generated files (idempotent)"""
        pb2_grpc_file = self.output_dir / "raft_pb2_grpc.py"

        if not pb2_grpc_file.exists():
            return

        content = pb2_grpc_file.read_text()

        # Nếu đã fix rồi thì không làm gì nữa
        if "from . import raft_pb2 as raft__pb2" in content:
            return

        # Chỉ replace import tuyệt đối
        content = content.replace(
            "import raft_pb2 as raft__pb2",
            "from . import raft_pb2 as raft__pb2"
        )

        pb2_grpc_file.write_text(content)
        logger.info("Fixed imports in raft_pb2_grpc.py")
    
    def prepare(self):
        """Chuẩn bị môi trường: clean và compile"""
        logger.info("Starting proto preparation...")
        # comment these 3 lines to run client, this code only for compile .proto
        # self.clean_generated_files()
        # self.compile_proto()
        # self.fix_imports()
        # -------------------------------------------------------------
        
        logger.info("Proto preparation completed!")
        return True


class RaftServicer:
    """Implementation của RAFT service"""
    
    def __init__(self, node):
        self.node = node
        logger.info(f"RaftServicer initialized for node: {node.node_id}")
    
    def RequestVote(self, request, context):
        """Handle RequestVote RPC"""
        logger.info(f"Received RequestVote from {request.candidate_id}, term={request.term}")
        
        # Import generated classes
        from src.generated import raft_pb2
        
        response = self.node.handle_request_vote(request)
        
        return raft_pb2.RequestVoteResponse(
            term=response['term'],
            vote_granted=response['vote_granted'],
            voter_id=self.node.node_id
        )
    
    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC"""
        from src.generated import raft_pb2
        # logger.info(f"[SERVER/AppendEntries] request =  {request}")
        
        if len(request.entries) > 0:
            logger.info(f"[Follower received AppendEntries] from {request.leader_id}, {len(request.entries)} entries")
        else:
            logger.info(f"[Follower received AppendEntries] [HEARTBEAT] from node {request.leader_id}")
        
        response = self.node.handle_append_entries(request)
        
        return raft_pb2.AppendEntriesResponse(
            term=response['term'],
            success=response['success'],
            match_index=response.get('match_index', 0),
            follower_id=self.node.node_id
        )
    
    def ClientRequest(self, request, context):
        """Handle client request"""
        from src.generated import raft_pb2
        
        logger.info(f"Received ClientRequest: {request.command}")
        
        response = self.node.handle_client_request(request)
        
        return raft_pb2.ClientResponseMsg(
            success=response['success'],
            result=response.get('result', ''),
            leader_id=response.get('leader_id', ''),
            error=response.get('error', '')
        )
    
    def DisconnectNodes(self, request, context):
        """Handle network partition simulation"""
        from src.generated import raft_pb2
        
        logger.warning(f"Disconnecting from nodes: {list(request.node_ids)}")
        
        self.node.disconnect_nodes(list(request.node_ids))
        
        return raft_pb2.DisconnectResponse(
            success=True,
            message=f"Disconnected from {len(request.node_ids)} nodes"
        )
    
    def Heartbeat(self, request, context):
        """Handle heartbeat check"""
        from src.generated import raft_pb2
        
        return raft_pb2.HeartbeatResponse(
            alive=True,
            term=self.node.current_term,
            state=self.node.state.name
        )
    
    def GetStatus(self, request, context):
        """Handle status request"""
        from src.generated import raft_pb2
        
        status = self.node.get_status()
        
        return raft_pb2.StatusResponse(
            node_id=status['node_id'],
            state=status['state'],
            current_term=status['current_term'],
            voted_for=status['voted_for'] or '',
            commit_index=status['commit_index'],
            last_applied=status['last_applied'],
            log_length=status['log_length'],
            cluster_nodes=status['cluster_nodes'],
            leader_id=status['leader_id'] or '',
            state_machine=status['state_machine']
        )


class RaftServer:
    """gRPC Server cho RAFT node"""
    
    def __init__(self, node, host='localhost', port=50050, max_workers=10):
        self.node = node
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.server = None
        self.servicer = None
        
        logger.info(f"Initializing RAFT server for {node.node_id} on {host}:{port}")
    
    def _prepare_proto(self):
        """Compile proto files trước khi start"""
        compiler = ProtoCompiler()
        compiler.prepare()
    
    def _setup_server(self):
        """Setup gRPC server"""
        # Import sau khi proto đã được compile
        from src.generated import raft_pb2_grpc
        
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=self.max_workers),
            options=[
                ('grpc.max_send_message_length', 50 * 1024 * 1024),  # 50MB
                ('grpc.max_receive_message_length', 50 * 1024 * 1024),  # 50MB
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
            ]
        )
        
        # Add servicer
        self.servicer = RaftServicer(self.node)
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self.servicer, self.server)
        
        # Bind port
        address = f'{self.host}:{self.port}'
        self.server.add_insecure_port(address)
        
        logger.info(f"Server configured at {address}")
    
    def start(self):
        """Start gRPC server"""
        try:
            # Step 1: Compile proto
            logger.info("=" * 60)
            logger.info("STEP 1: Compiling proto files...")
            logger.info("=" * 60)
            self._prepare_proto()
            
            # Step 2: Setup server
            logger.info("=" * 60)
            logger.info("STEP 2: Setting up gRPC server...")
            logger.info("=" * 60)
            self._setup_server()
            
            # Step 3: Start server
            logger.info("=" * 60)
            logger.info("STEP 3: Starting server...")
            logger.info("=" * 60)
            self.server.start()
            
            logger.info("=" * 60)
            logger.info(f"✓ RAFT Server '{self.node.node_id}' is running on {self.host}:{self.port}")
            logger.info("=" * 60)
            
            # Start node logic
            self.node.start()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start server: {e}", exc_info=True)
            raise
    
    def stop(self, grace_period=5):
        """Stop gRPC server"""
        if self.server:
            logger.info(f"Stopping server for {self.node.node_id}...")
            self.node.stop()
            self.server.stop(grace_period)
            logger.info("Server stopped")
    
    def wait_for_termination(self):
        """Wait for server termination"""
        if self.server:
            try:
                self.server.wait_for_termination()
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                self.stop()


def create_server(node, host='localhost', port=50050, max_workers=10):
    """Factory function để tạo RAFT server"""
    return RaftServer(node, host, port, max_workers)


# Example usage
if __name__ == "__main__":
    # Mock node để test server
    class MockNode:
        def __init__(self, node_id):
            self.node_id = node_id
            self.current_term = 0
            self.state = type('State', (), {'name': 'FOLLOWER'})()
        
        def start(self):
            logger.info(f"Node {self.node_id} started")
        
        def stop(self):
            logger.info(f"Node {self.node_id} stopped")
        
        def handle_request_vote(self, request):
            return {'term': self.current_term, 'vote_granted': True}
        
        def handle_append_entries(self, request):
            return {'term': self.current_term, 'success': True, 'match_index': 0}
        
        def handle_client_request(self, request):
            return {'success': True, 'result': 'OK'}
        
        def disconnect_nodes(self, node_ids):
            pass
        
        def get_status(self):
            return {
                'node_id': self.node_id,
                'state': 'FOLLOWER',
                'current_term': 0,
                'voted_for': None,
                'commit_index': 0,
                'last_applied': 0,
                'log_length': 0,
                'cluster_nodes': [],
                'leader_id': None,
                'state_machine': {}
            }
    
    # Test server
    node = MockNode('test_node')
    server = create_server(node, port=50050)
    
    try:
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.stop()
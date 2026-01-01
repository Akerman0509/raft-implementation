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
            # Step 2: Setup server
            logger.info("=" * 60)
            logger.info("STEP 1: Setting up gRPC server...")
            logger.info("=" * 60)
            self._setup_server()
            
            # Step 3: Start server
            logger.info("=" * 60)
            logger.info("STEP 2: Starting server...")
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
    pass
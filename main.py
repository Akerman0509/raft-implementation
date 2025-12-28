#!/usr/bin/env python3
"""
RAFT Node Entry Point
Khởi động một RAFT node với gRPC server
"""

import sys
import argparse
import logging
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.raft_node import RaftNode
from src.server import create_server

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_config(config_file='config/cluster_config.yaml'):
    """Load cluster configuration"""
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Start a RAFT node',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start node1
  python main.py --node-id node1 --port 50051
  
  # Start with custom config
  python main.py --node-id node2 --port 50052 --config my_config.yaml
  
  # Start with debug logging
  python main.py --node-id node3 --port 50053 --debug
        """
    )
    
    parser.add_argument(
        '--node-id',
        type=str,
        required=True,
        help='Unique identifier for this node (e.g., node1, node2)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        required=True,
        help='Port for gRPC server (e.g., 50051, 50052)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Host address (default: localhost)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config/cluster_config.yaml',
        help='Path to cluster config file'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    return parser.parse_args()


def setup_logging(node_id, debug=False):
    """Setup logging cho node"""
    level = logging.DEBUG if debug else logging.INFO
    
    # Update root logger
    logging.getLogger().setLevel(level)
    
    # Tạo log directory
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Add file handler
    file_handler = logging.FileHandler(log_dir / f'{node_id}.log',mode='w')
    file_handler.setLevel(level)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logging.getLogger().addHandler(file_handler)
    
    logger.info(f"Logging configured for {node_id} (level: {logging.getLevelName(level)})")


def create_data_directory(node_id):
    """Tạo data directory cho node"""
    data_dir = Path('data') / node_id
    data_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Data directory: {data_dir}")
    return data_dir


def print_startup_banner(node_id, host, port):
    """In banner khi start"""
    banner = f"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                    🏛️  RAFT NODE STARTING                     ║
║                                                               ║
║  Node ID:  {node_id:<49} ║
║  Address:  {f"{host}:{port}":<49} ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    setup_logging(args.node_id, args.debug)
    
    try:
        # Print banner
        print_startup_banner(args.node_id, args.host, args.port)
        
        # Load config
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)
        
        # Create data directory
        data_dir = create_data_directory(args.node_id)
        
        # Create RAFT node
        logger.info("Creating RAFT node...")
        node = RaftNode(
            node_id=args.node_id,
            host=args.host,
            port=args.port,
            cluster_config=config,
            data_dir=data_dir
        )
        
        # Create gRPC server
        logger.info("Creating gRPC server...")
        server = create_server(
            node=node,
            host=args.host,
            port=args.port,
            max_workers=10
        )
        
        # Start server (tự động compile proto)
        logger.info("Starting server...")
        server.start()
        
        logger.info("=" * 70)
        logger.info(f"✅ Node {args.node_id} is ready and running!")
        logger.info(f"   Listening on {args.host}:{args.port}")
        logger.info("=" * 70)
        
        # Wait for termination
        server.wait_for_termination()
        
    except KeyboardInterrupt:
        logger.info(f"\n⚠️  Node {args.node_id} received shutdown signal")
        if 'server' in locals():
            server.stop()
        logger.info(f"✅ Node {args.node_id} stopped gracefully")
        
    except FileNotFoundError as e:
        logger.error(f"❌ Configuration error: {e}")
        logger.error("Make sure cluster_config.yaml exists in config/")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Start RAFT Cluster - Khởi động toàn bộ cluster với rich logging
"""

import subprocess
import time
import sys
import os
import signal
import yaml
from pathlib import Path

try:
    from colorama import Fore, Style, init
    init(autoreset=True)
    COLORS = True
except ImportError:
    COLORS = False


class ClusterManager:
    """Quản lý lifecycle của RAFT cluster"""
    
    def __init__(self, config_file='config/cluster_config.yaml'):
        self.config_file = config_file
        self.config = self.load_config()
        self.processes = []
        self.root_dir = Path(__file__).parent.parent
        
    def load_config(self):
        """Load cluster configuration"""
        config_path = Path(self.config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def colorize(self, text, color='white'):
        """Add color to text"""
        if not COLORS:
            return text
        
        colors = {
            'red': Fore.RED,
            'green': Fore.GREEN,
            'yellow': Fore.YELLOW,
            'blue': Fore.BLUE,
            'cyan': Fore.CYAN,
            'white': Fore.WHITE,
        }
        return colors.get(color, Fore.WHITE) + text + Style.RESET_ALL
    
    def print_banner(self):
        """Print startup banner"""
        banner = """
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║              🏛️  RAFT CLUSTER MANAGER 🏛️                     ║
║                                                               ║
║              Starting Distributed Consensus Cluster          ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
        """
        print(self.colorize(banner, 'cyan'))
    
    def create_directories(self):
        """Tạo các thư mục cần thiết"""
        dirs = [
            'data',
            'logs',
            'src/generated'
        ]
        
        print(self.colorize("\n📁 Creating directories...", 'yellow'))
        
        for dir_name in dirs:
            dir_path = self.root_dir / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"  ✓ {dir_name}")
        
        # Create node-specific directories
        for node in self.config['cluster']['nodes']:
            node_dir = self.root_dir / 'data' / node['id']
            node_dir.mkdir(parents=True, exist_ok=True)
            print(f"  ✓ data/{node['id']}")
    
    def check_ports_available(self):
        """Kiểm tra các ports có sẵn không"""
        print(self.colorize("\n🔍 Checking port availability...", 'yellow'))
        
        import socket
        
        all_available = True
        for node in self.config['cluster']['nodes']:
            port = node['port']
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            
            if result == 0:
                print(self.colorize(f"  ✗ Port {port} ({node['id']}) is already in use", 'red'))
                all_available = False
            else:
                print(self.colorize(f"  ✓ Port {port} ({node['id']}) is available", 'green'))
        
        return all_available
    
    def start_node(self, node_config):
        """Start một RAFT node"""
        node_id = node_config['id']
        port = node_config['port']
        
        # Command để start node
        cmd = [
            sys.executable,
            'main.py',
            '--node-id', node_id,
            '--port', str(port),
            '--config', self.config_file
        ]
        
        # Log file cho node
        log_file = self.root_dir / 'logs' / f'{node_id}.log'
        
        print(f"  Starting {self.colorize(node_id, 'cyan')} on port {self.colorize(str(port), 'green')}...", end=' ')
        
        try:
            # Start process
            with open(log_file, 'w') as log:
                process = subprocess.Popen(
                    cmd,
                    cwd=self.root_dir,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid if sys.platform != 'win32' else None
                )
            
            self.processes.append({
                'node_id': node_id,
                'process': process,
                'port': port,
                'log_file': log_file
            })
            
            print(self.colorize("✓", 'green'))
            return True
            
        except Exception as e:
            print(self.colorize(f"✗ Failed: {e}", 'red'))
            return False
    
    def start_all_nodes(self):
        """Start tất cả nodes trong cluster"""
        print(self.colorize("\n🚀 Starting cluster nodes...\n", 'yellow'))
        
        success_count = 0
        for node in self.config['cluster']['nodes']:
            if self.start_node(node):
                success_count += 1
            time.sleep(0.5)  # Delay giữa các nodes
        
        total_nodes = len(self.config['cluster']['nodes'])
        print(
            f"\n  {self.colorize(f'Started {success_count}/{total_nodes} nodes', 'green')}"
        )
        
        if success_count == len(self.config['cluster']['nodes']):
            print(self.colorize("\n  ✅ All nodes started successfully!", 'green'))
            return True
        else:
            print(self.colorize("\n  ⚠️  Some nodes failed to start", 'yellow'))
            return False
    
    def wait_for_cluster_ready(self, timeout=30):
        """Đợi cluster sẵn sàng"""
        print(self.colorize(f"\n⏳ Waiting for cluster to be ready (timeout: {timeout}s)...", 'yellow'))
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check if processes are still running
            running = sum(1 for p in self.processes if p['process'].poll() is None)
            
            print(f"  {running}/{len(self.processes)} nodes running...", end='\r')
            
            if running == len(self.processes):
                time.sleep(2)
                print(self.colorize(f"\n  ✅ Cluster is ready! ({len(self.processes)} nodes)", 'green'))
                return True
            
            time.sleep(1)
        
        print(self.colorize("\n  ⚠️  Cluster not fully ready", 'yellow'))
        return False
    
    def show_cluster_info(self):
        """Hiển thị thông tin cluster"""
        print(self.colorize("\n📊 Cluster Information:", 'cyan'))
        print("=" * 70)
        
        for proc_info in self.processes:
            status = "Running" if proc_info['process'].poll() is None else "Stopped"
            color = 'green' if status == "Running" else 'red'
            
            print(f"  • {self.colorize(proc_info['node_id'], 'cyan')}")
            print(f"    Port: {proc_info['port']}")
            print(f"    Status: {self.colorize(status, color)}")
            print(f"    Log: {proc_info['log_file']}")
            print(f"    PID: {proc_info['process'].pid}")
            print()
        
        print("=" * 70)
    
    def show_usage_info(self):
        """Hiển thị hướng dẫn sử dụng"""
        print(self.colorize("\n📖 Usage Information:", 'cyan'))
        print("=" * 70)
        print()
        print("  Monitor cluster:")
        print(self.colorize("    python scripts/monitor.py", 'yellow'))
        print()
        print("  Watch mode (auto-refresh):")
        print(self.colorize("    python scripts/monitor.py watch", 'yellow'))
        print()
        print("  View logs:")
        print(self.colorize("    tail -f logs/node1.log", 'yellow'))
        print()
        print("  Stop cluster:")
        print(self.colorize("    python scripts/stop_cluster.py", 'yellow'))
        print(self.colorize("    # or press Ctrl+C in this terminal", 'yellow'))
        print()
        print("=" * 70)
    
    def stop_all_nodes(self):
        """Stop tất cả nodes"""
        print(self.colorize("\n🛑 Stopping cluster...", 'yellow'))
        
        for proc_info in self.processes:
            node_id = proc_info['node_id']
            process = proc_info['process']
            
            if process.poll() is None:
                print(f"  Stopping {node_id}...", end=' ')
                
                try:
                    if sys.platform == 'win32':
                        process.terminate()
                    else:
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    
                    process.wait(timeout=5)
                    print(self.colorize("✓", 'green'))
                except Exception as e:
                    print(self.colorize(f"✗ ({e})", 'red'))
        
        self.processes.clear()
        print(self.colorize("\n  ✅ Cluster stopped", 'green'))
    
    def run(self):
        """Main run method"""
        try:
            self.print_banner()
            
            # Step 1: Create directories
            self.create_directories()
            
            # Step 2: Check ports
            if not self.check_ports_available():
                print(self.colorize("\n❌ Some ports are not available. Please stop existing processes.", 'red'))
                return False
            
            # Step 3: Start nodes
            if not self.start_all_nodes():
                print(self.colorize("\n❌ Failed to start all nodes", 'red'))
                return False
            
            # Step 4: Wait for ready
            self.wait_for_cluster_ready()
            
            # Step 5: Show info
            self.show_cluster_info()
            self.show_usage_info()
            
            # Keep running
            print(self.colorize("\n✨ Cluster is running! Press Ctrl+C to stop.\n", 'green'))
            
            try:
                while True:
                    time.sleep(1)
                    # Check if any process died
                    for proc_info in self.processes:
                        if proc_info['process'].poll() is not None:
                            print(self.colorize(f"\n⚠️  Node {proc_info['node_id']} died unexpectedly!", 'red'))
            
            except KeyboardInterrupt:
                print(self.colorize("\n\n⚠️  Received stop signal...", 'yellow'))
            
            return True
            
        finally:
            self.stop_all_nodes()


def main():
    """Main entry point"""
    try:
        manager = ClusterManager()
        success = manager.run()
        sys.exit(0 if success else 1)
    
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Make sure you're running from the project root directory.")
        sys.exit(1)
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
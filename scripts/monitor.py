#!/usr/bin/env python3
"""
RAFT Cluster Monitor - Real-time monitoring and testing tool
"""

import sys
import os
import time
import yaml
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client import RaftClient, RaftClientPool

try:
    from colorama import Fore, Back, Style, init
    init(autoreset=True)
    COLORS_AVAILABLE = True
except ImportError:
    print("Install colorama for colored output: pip install colorama")
    COLORS_AVAILABLE = False


class ClusterMonitor:
    """Real-time cluster monitoring với rich output"""
    
    def __init__(self, config_file='config/cluster_config.yaml'):
        self.config = self.load_config(config_file)
        self.pool = RaftClientPool(self.config)
        self.setup_cluster()
        
    def load_config(self, config_file):
        """Load cluster configuration"""
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def setup_cluster(self):
        """Setup client connections"""
        for node in self.config['cluster']['nodes']:
            self.pool.add_node(node['id'], node['host'], node['port'])
    
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def colorize(self, text, color='white'):
        """Add color to text"""
        if not COLORS_AVAILABLE:
            return text
        
        colors = {
            'red': Fore.RED,
            'green': Fore.GREEN,
            'yellow': Fore.YELLOW,
            'blue': Fore.BLUE,
            'magenta': Fore.MAGENTA,
            'cyan': Fore.CYAN,
            'white': Fore.WHITE,
            'gold': Fore.YELLOW + Style.BRIGHT,
        }
        return colors.get(color, Fore.WHITE) + text + Style.RESET_ALL
    
    def print_header(self):
        """Print dashboard header"""
        print("\n" + "=" * 80)
        print(self.colorize("🏛️  RAFT CLUSTER MONITOR", 'cyan') + " " * 30 + 
              self.colorize(f"[{datetime.now().strftime('%H:%M:%S')}]", 'white'))
        print("=" * 80 + "\n")
    
    def print_node_status(self, node_id, status):
        """Print single node status"""
        if not status:
            print(f"  {self.colorize('❌ ' + node_id, 'red'):<50} DISCONNECTED")
            return
        
        # State emoji and color
        state_info = {
            'LEADER': ('👑', 'gold'),
            'CANDIDATE': ('🗳️ ', 'yellow'),
            'FOLLOWER': ('👥', 'green'),
        }
        
        emoji, color = state_info.get(status['state'], ('❓', 'white'))
        
        # Node header
        node_header = f"  {emoji} {self.colorize(node_id, color)}"
        state_badge = self.colorize(f"[{status['state']}]", color)
        print(f"{node_header:<60} {state_badge}")
        
        # Node details
        print(f"     Term: {self.colorize(str(status['current_term']), 'cyan')}", end="  ")
        print(f"Leader: {self.colorize(status['leader_id'] or 'None', 'yellow')}", end="  ")
        print(f"Commit: {self.colorize(str(status['commit_index']), 'green')}", end="  ")
        print(f"Log: {self.colorize(str(status['log_length']), 'magenta')}")
        
        # Key-value store preview
        if status['state_machine']:
            kv_count = len(status['state_machine'])
            print(f"     KV Store: {self.colorize(f'{kv_count} items', 'blue')}", end="")
            
            # Show first 3 items
            items = list(status['state_machine'].items())[:3]
            if items:
                print(" - ", end="")
                print(", ".join([f"{k}={v}" for k, v in items]))
            else:
                print()
        
        print()
    
    def show_cluster_status(self):
        """Show complete cluster status"""
        self.clear_screen()
        self.print_header()
        
        leader_count = 0
        connected_count = 0
        
        for node_id in self.pool.clients.keys():
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                if status:
                    self.print_node_status(node_id, status)
                    connected_count += 1
                    if status['state'] == 'LEADER':
                        leader_count += 1
                else:
                    self.print_node_status(node_id, None)
            else:
                self.print_node_status(node_id, None)
        
        # Summary
        print("=" * 80)
        print(f"  {self.colorize('Summary:', 'cyan')}", end="  ")
        print(f"Nodes: {connected_count}/{len(self.pool.clients)}", end="  ")
        print(f"Leaders: {self.colorize(str(leader_count), 'gold')}", end="  ")
        
        if leader_count == 0:
            print(self.colorize("⚠️  NO LEADER!", 'red'))
        elif leader_count > 1:
            print(self.colorize("⚠️  MULTIPLE LEADERS!", 'red'))
        else:
            print(self.colorize("✅ Healthy", 'green'))
        
        print("=" * 80)
    
    def watch_mode(self, interval=3):
        """Continuous monitoring mode"""
        print(self.colorize("\n🔄 Starting watch mode (Ctrl+C to stop)...\n", 'cyan'))
        time.sleep(1)
        
        try:
            while True:
                self.show_cluster_status()
                print(f"\n  Refreshing in {interval}s... (Press Ctrl+C to stop)")
                time.sleep(interval)
        except KeyboardInterrupt:
            print(self.colorize("\n\n👋 Monitoring stopped.\n", 'yellow'))
    
    def test_leader_election(self):
        """Test leader election"""
        self.clear_screen()
        self.print_header()
        print(self.colorize("🗳️  TESTING LEADER ELECTION\n", 'yellow'))
        
        # Find current leader
        leader_id = None
        for node_id in self.pool.clients.keys():
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                if status and status['state'] == 'LEADER':
                    leader_id = node_id
                    break
        
        if not leader_id:
            print(self.colorize("❌ No leader found in cluster", 'red'))
            return
        
        print(f"  Current leader: {self.colorize(leader_id, 'gold')}")
        print(f"  {self.colorize('💥 Simulating leader failure...', 'red')}")
        
        # Disconnect leader
        self.pool.disconnect_node(leader_id)
        print(f"  ✅ Leader {leader_id} disconnected")
        
        # Wait for new election
        print(f"\n  {self.colorize('⏳ Waiting for new election (10s)...', 'yellow')}")
        for i in range(10, 0, -1):
            print(f"  {i}...", end="\r")
            time.sleep(1)
        
        print("\n\n  {self.colorize('📊 Checking new cluster state:', 'cyan')}\n")
        
        # Show new status
        new_leader = None
        for node_id in self.pool.clients.keys():
            if node_id == leader_id:
                continue
            
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                if status:
                    self.print_node_status(node_id, status)
                    if status['state'] == 'LEADER':
                        new_leader = node_id
        
        if new_leader:
            print(f"\n  {self.colorize('✅ New leader elected: ' + new_leader, 'green')}")
        else:
            print(f"\n  {self.colorize('❌ No new leader elected yet', 'red')}")
        
        print("\n" + "=" * 80)
        input("\nPress Enter to continue...")
    
    def test_log_replication(self):
        """Test log replication"""
        self.clear_screen()
        self.print_header()
        print(self.colorize("📝 TESTING LOG REPLICATION\n", 'yellow'))
        
        # Find leader
        leader_id = None
        for node_id in self.pool.clients.keys():
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                if status and status['state'] == 'LEADER':
                    leader_id = node_id
                    break
        
        if not leader_id:
            print(self.colorize("❌ No leader found", 'red'))
            input("\nPress Enter to continue...")
            return
        
        print(f"  Leader: {self.colorize(leader_id, 'gold')}\n")
        
        # Send commands
        commands = [
            "SET user1 Alice",
            "SET user2 Bob",
            "SET counter 42",
            "GET user1",
            "DELETE user2"
        ]
        
        print(f"  {self.colorize('Sending commands:', 'cyan')}\n")
        
        client = self.pool.get_client(leader_id)
        for cmd in commands:
            print(f"    → {cmd}")
            response = client.client_request(cmd)
            if response and response['success']:
                print(f"      {self.colorize('✅ Success', 'green')}: {response.get('result', 'OK')}")
            else:
                print(f"      {self.colorize('❌ Failed', 'red')}")
            time.sleep(0.5)
        
        # Check replication
        print(f"\n  {self.colorize('⏳ Waiting for replication (3s)...', 'yellow')}")
        time.sleep(3)
        
        print(f"\n  {self.colorize('Checking all nodes:', 'cyan')}\n")
        
        for node_id in self.pool.clients.keys():
            client = self.pool.get_client(node_id)
            if client:
                status = client.get_status()
                if status:
                    kv = status.get('state_machine', {})
                    print(f"    {node_id}: {self.colorize(str(len(kv)) + ' items', 'green')}")
                    for k, v in list(kv.items())[:3]:
                        print(f"      • {k} = {v}")
        
        print("\n" + "=" * 80)
        input("\nPress Enter to continue...")
    
    def interactive_menu(self):
        """Interactive menu"""
        while True:
            self.clear_screen()
            self.print_header()
            
            print(self.colorize("📋 MENU\n", 'cyan'))
            print("  1. Show cluster status (once)")
            print("  2. Watch mode (continuous)")
            print("  3. Test leader election")
            print("  4. Test log replication")
            print("  5. Send custom command")
            print("  0. Exit")
            print()
            
            choice = input(self.colorize("Select option: ", 'yellow'))
            
            if choice == '1':
                self.show_cluster_status()
                input("\nPress Enter to continue...")
            
            elif choice == '2':
                self.watch_mode()
            
            elif choice == '3':
                self.test_leader_election()
            
            elif choice == '4':
                self.test_log_replication()
            
            elif choice == '5':
                self.clear_screen()
                self.print_header()
                print(self.colorize("📝 SEND CUSTOM COMMAND\n", 'cyan'))
                
                nodes = list(self.pool.clients.keys())
                for i, node_id in enumerate(nodes, 1):
                    print(f"  {i}. {node_id}")
                
                try:
                    node_idx = int(input("\nSelect node: ")) - 1
                    node_id = nodes[node_idx]
                    
                    command = input("Enter command: ")
                    
                    client = self.pool.get_client(node_id)
                    if client:
                        response = client.client_request(command)
                        if response:
                            if response['success']:
                                print(f"\n{self.colorize('✅ Success', 'green')}: {response.get('result', 'OK')}")
                            else:
                                print(f"\n{self.colorize('❌ Failed', 'red')}: {response.get('error', 'Unknown error')}")
                        else:
                            print(f"\n{self.colorize('❌ No response', 'red')}")
                    
                    input("\nPress Enter to continue...")
                    
                except (ValueError, IndexError):
                    print(self.colorize("\n❌ Invalid selection", 'red'))
                    input("\nPress Enter to continue...")
            
            elif choice == '0':
                print(self.colorize("\n👋 Goodbye!\n", 'cyan'))
                break
            
            else:
                print(self.colorize("\n❌ Invalid option", 'red'))
                time.sleep(1)
        
        self.pool.close_all()


def main():
    """Main entry point"""
    try:
        monitor = ClusterMonitor()
        
        if len(sys.argv) > 1:
            if sys.argv[1] == 'watch':
                interval = int(sys.argv[2]) if len(sys.argv) > 2 else 3
                monitor.watch_mode(interval)
            elif sys.argv[1] == 'status':
                monitor.show_cluster_status()
        else:
            monitor.interactive_menu()
    
    except FileNotFoundError as e:
        print(f"❌ Config file not found: {e}")
        print("Make sure cluster_config.yaml exists in config/")
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user\n")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
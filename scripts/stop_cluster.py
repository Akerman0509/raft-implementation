#!/usr/bin/env python3
"""
Stop RAFT Cluster
Dừng tất cả các nodes trong cluster
"""

import os
import signal
import sys
import psutil
import time
from pathlib import Path

try:
    from colorama import Fore, Style, init
    init(autoreset=True)
    COLORS = True
except ImportError:
    COLORS = False


def colorize(text, color='white'):
    """Add color to text"""
    if not COLORS:
        return text
    
    colors = {
        'red': Fore.RED,
        'green': Fore.GREEN,
        'yellow': Fore.YELLOW,
        'cyan': Fore.CYAN,
    }
    return colors.get(color, Fore.WHITE) + text + Style.RESET_ALL


def print_banner():
    """Print stop banner"""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                🛑 STOPPING RAFT CLUSTER 🛑                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """
    print(colorize(banner, 'yellow'))


def find_raft_processes():
    """Find all RAFT node processes"""
    raft_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and 'main.py' in ' '.join(cmdline) and '--node-id' in ' '.join(cmdline):
                # Extract node_id
                node_id = None
                for i, arg in enumerate(cmdline):
                    if arg == '--node-id' and i + 1 < len(cmdline):
                        node_id = cmdline[i + 1]
                        break
                
                raft_processes.append({
                    'pid': proc.info['pid'],
                    'node_id': node_id,
                    'cmdline': ' '.join(cmdline)
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    return raft_processes


def stop_process(pid, node_id):
    """Stop a process gracefully"""
    try:
        process = psutil.Process(pid)
        
        # Try graceful termination first
        print(f"  Stopping {colorize(node_id, 'cyan')} (PID: {pid})...", end=' ')
        
        process.terminate()
        
        # Wait for process to terminate
        try:
            process.wait(timeout=5)
            print(colorize("✓", 'green'))
            return True
        except psutil.TimeoutExpired:
            # Force kill if it doesn't terminate
            print(colorize("⚠ Force killing...", 'yellow'), end=' ')
            process.kill()
            process.wait(timeout=2)
            print(colorize("✓", 'green'))
            return True
    
    except psutil.NoSuchProcess:
        print(colorize("✓ Already stopped", 'green'))
        return True
    
    except Exception as e:
        print(colorize(f"✗ Failed: {e}", 'red'))
        return False


def clean_log_files():
    """Clean up log files"""
    log_dir = Path('logs')
    if log_dir.exists():
        log_files = list(log_dir.glob('*.log'))
        if log_files:
            print(f"\n{colorize('📁 Found log files:', 'cyan')}")
            for log_file in log_files:
                size = log_file.stat().st_size
                print(f"  • {log_file.name} ({size} bytes)")
            
            response = input(f"\n{colorize('Do you want to delete log files? (y/N): ', 'yellow')}")
            if response.lower() == 'y':
                for log_file in log_files:
                    try:
                        log_file.unlink()
                        print(f"  {colorize('✓', 'green')} Deleted {log_file.name}")
                    except Exception as e:
                        print(f"  {colorize('✗', 'red')} Failed to delete {log_file.name}: {e}")


def main():
    """Main entry point"""
    print_banner()
    
    # Find RAFT processes
    print(colorize("\n🔍 Finding RAFT node processes...", 'yellow'))
    processes = find_raft_processes()
    
    if not processes:
        print(colorize("\n✓ No RAFT nodes running", 'green'))
        return
    
    print(f"\n{colorize(f'Found {len(processes)} RAFT node(s):', 'cyan')}")
    for proc in processes:
        print(f"  • {proc['node_id']} (PID: {proc['pid']})")
    
    # Confirm
    print()
    response = input(colorize("Stop all nodes? (Y/n): ", 'yellow'))
    
    if response.lower() == 'n':
        print(colorize("\n⚠️  Cancelled", 'yellow'))
        return
    
    # Stop all processes
    print(f"\n{colorize('🛑 Stopping nodes...', 'yellow')}\n")
    
    success_count = 0
    for proc in processes:
        if stop_process(proc['pid'], proc['node_id']):
            success_count += 1
        time.sleep(0.2)
    
    # Summary
    print(f"\n{colorize('=' * 70, 'cyan')}")
    if success_count == len(processes):
        print(colorize(f"✅ Successfully stopped all {success_count} nodes", 'green'))
    else:
        print(colorize(f"⚠️  Stopped {success_count}/{len(processes)} nodes", 'yellow'))
    print(colorize('=' * 70, 'cyan'))
    
    # Clean logs
    clean_log_files()
    
    print(colorize("\n👋 Cluster stopped\n", 'cyan'))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(colorize("\n\n⚠️  Interrupted by user\n", 'yellow'))
        sys.exit(1)
    except Exception as e:
        print(colorize(f"\n❌ Error: {e}\n", 'red'))
        sys.exit(1)
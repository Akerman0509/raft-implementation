import time
import yaml
import logging
from typing import Optional, Tuple, Dict, Any
from src.client import RaftClientPool

# --- CONFIGURATION ---
CONFIG_FILE = 'config/cluster_config.yaml'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("RaftTester")

class RaftClusterTest:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.pool = RaftClientPool(self.config)
        self.nodes = self.config['cluster']['nodes']
        self._init_pool_connections()

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def _save_config(self, config: Dict[str, Any]):
        with open(self.config_path, 'w') as f:
            yaml.dump(config, f)

    def _init_pool_connections(self):
        """Khởi tạo kết nối ban đầu cho pool"""
        for node in self.nodes:
            self.pool.add_node(node['id'], node['host'], node['port'])

    def set_node_status(self, node_id: str, status: str):
        """Cập nhật trạng thái node (up/down) và lưu vào file"""
        try:
            config = self._load_config()
            for node in config['cluster']['nodes']:
                if node['id'] == node_id:
                    node['status'] = status
            
            self._save_config(config)
            logger.info(f"⚡ Đã cập nhật trạng thái {node_id} -> {status.upper()}")
            
            # Cập nhật lại biến nodes cục bộ
            self.nodes = config['cluster']['nodes']
            time.sleep(2) # Chờ hệ thống ổn định sau khi đổi trạng thái
        except Exception as e:
            logger.error(f"Lỗi khi update config: {e}")

    def reset_cluster(self):
        """Hồi phục toàn bộ node về trạng thái UP"""
        logger.info("🔄 Reset trạng thái toàn bộ Cluster...")
        for node in self.nodes:
            self.set_node_status(node['id'], 'up')
        time.sleep(3) # Chờ bầu cử ổn định

    def get_stable_leader(self, timeout: int = 15) -> Tuple[Optional[str], Any]:
        """
        Tìm Leader ổn định.
        Trả về: (leader_id, leader_client)
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Lấy danh sách các node đang UP thực tế từ file config
            current_config = self._load_config()
            active_nodes = [n for n in current_config['cluster']['nodes'] if n['status'] == 'up']
            
            for node in active_nodes:
                try:
                    client = self.pool.get_client(node['id'])
                    if not client: continue
                    
                    status = client.get_status()
                    if status and status.get('state') == 'LEADER':
                        return node['id'], client
                except Exception:
                    continue
            
            time.sleep(1)
            logger.debug("... Đang tìm Leader ...")
        
        return None, None

    def wait_for_data_sync(self, node_id: str, key: str, expected_val: str, timeout: int = 15) -> bool:
        """Chờ một node cụ thể đồng bộ dữ liệu"""
        start = time.time()
        client = self.pool.get_client(node_id)
        
        while time.time() - start < timeout:
            try:
                status = client.get_status()
                val = status.get('state_machine', {}).get(key)
                if str(val) == str(expected_val):
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        
        return False

    # ================= TEST CASES =================

    def test_log_replication(self):
        logger.info("\n" + "="*50)
        logger.info("🧪 TEST CASE 1: LOG REPLICATION & CATCH-UP")
        logger.info("="*50)

        self.reset_cluster()

        # 1. Tìm Leader
        leader_id, leader_client = self.get_stable_leader()
        if not leader_id:
            logger.error("❌ Không thể tìm thấy Leader để bắt đầu test.")
            return

        logger.info(f"👑 Leader hiện tại: {leader_id}")

        # 2. Ghi dữ liệu mẫu
        logger.info("📝 Gửi lệnh: SET x = 100")
        leader_client.client_request("SET x 100")

        # 3. Chọn nạn nhân (Follower)
        followers = [n['id'] for n in self.nodes if n['id'] != leader_id]
        victim = followers[0]
        
        logger.info(f"💀 Giết Follower: {victim}")
        self.set_node_status(victim, 'down')

        # 4. Ghi dữ liệu trong khi nạn nhân chết
        logger.info("📝 Gửi lệnh khi node chết: SET y=200, z=300")
        leader_client.client_request("SET y 200")
        leader_client.client_request("SET z 300")

        # 5. Hồi sinh
        logger.info(f"🚑 Hồi sinh node: {victim}")
        self.set_node_status(victim, 'up')

        # 6. Verify
        logger.info(f"⏳ Kiểm tra tính nhất quán trên {victim}...")
        is_synced = self.wait_for_data_sync(victim, 'z', '300')
        
        if is_synced:
            logger.info(f"✅ PASSED: Node {victim} đã catch-up dữ liệu thành công!")
        else:
            logger.error(f"❌ FAILED: Node {victim} mất dữ liệu hoặc không đồng bộ kịp.")

    def test_failure_threshold(self):
        logger.info("\n" + "="*50)
        logger.info("🧪 TEST CASE 2: FAILURE THRESHOLD (QUORUM)")
        logger.info("="*50)

        self.reset_cluster()
        
        total_nodes = len(self.nodes)
        quorum = (total_nodes // 2) + 1
        dead_count = 0
        
        logger.info(f"📊 Cluster: {total_nodes} nodes | Quorum cần: {quorum}")

        # Kill từng node một cho đến khi sập toàn bộ
        while dead_count < total_nodes:
            alive_count = total_nodes - dead_count
            logger.info(f"\n--- Kiểm tra với {alive_count} node sống ---")

            # 1. Cố gắng tìm Leader trong đám còn sống
            leader_id, leader_client = self.get_stable_leader(timeout=5)

            # 2. Kiểm tra khả năng ghi (Write Availability)
            if alive_count >= quorum:
                if leader_id:
                    logger.info(f"✅ Quorum OK ({alive_count} >= {quorum}). Leader: {leader_id}")
                    # Thử ghi
                    resp = leader_client.client_request(f"SET check_{alive_count} ok")
                    if resp and resp.get('success'):
                        logger.info("   -> Ghi dữ liệu: THÀNH CÔNG")
                    else:
                        logger.warning(f"   -> Ghi dữ liệu: THẤT BẠI (Lỗi: {resp})")
                else:
                    logger.error(f"❌ Quorum đủ nhưng KHÔNG bầu được Leader!")
            else:
                logger.info(f"🛑 Mất Quorum ({alive_count} < {quorum}). Hệ thống phải dừng hoạt động.")
                if leader_id:
                    logger.warning(f"   ⚠️ CẢNH BÁO: Vẫn tìm thấy Leader {leader_id} (Split brain?)")
                else:
                    logger.info("   -> Đúng dự kiến: Không có Leader.")

            # 3. Chọn nạn nhân tiếp theo (Ưu tiên giết Leader nếu có để ép bầu cử lại)
            current_alive_nodes = [n['id'] for n in self.nodes if n['status'] == 'up']
            if not current_alive_nodes: break

            next_victim = leader_id if leader_id in current_alive_nodes else current_alive_nodes[0]
            
            logger.info(f"🔻 Tắt node: {next_victim}")
            self.set_node_status(next_victim, 'down')
            dead_count += 1
            
            # Nếu đã mất quorum thì không cần chờ lâu, ngược lại chờ bầu cử
            time.sleep(2 if alive_count < quorum else 4)

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    tester = RaftClusterTest(CONFIG_FILE)
    
    try:
        tester.test_log_replication()
        tester.test_failure_threshold()
    except KeyboardInterrupt:
        logger.info("\n⏹️ Test bị hủy bởi người dùng.")
    except Exception as e:
        logger.exception(f"💥 Lỗi không mong muốn: {e}")
    finally:
        # Luôn dọn dẹp hiện trường sau khi test xong (kể cả khi lỗi)
        tester.reset_cluster()
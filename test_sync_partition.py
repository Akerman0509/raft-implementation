import time
import yaml
import logging
import uuid
from contextlib import contextmanager
from typing import Optional, List, Tuple
from src.client import RaftClientPool

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PartitionTest")

CONFIG_PATH = 'config/cluster_config.yaml'

class ClusterController:
    """Quản lý các thao tác với Cluster (Config, Client, Leader)"""
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
        self.pool = RaftClientPool(self.config)
        self.nodes = self.config['cluster']['nodes']
        self._init_connections()

    def _load_config(self):
        with open(self.config_file, 'r') as f:
            return yaml.safe_load(f)

    def _save_config(self, config):
        with open(self.config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

    def _init_connections(self):
        for node in self.nodes:
            self.pool.add_node(node['id'], node['host'], node['port'])

    def set_partition_mode(self, enabled: bool):
        """Cập nhật file config để Bật/Tắt partition"""
        try:
            config = self._load_config()
            # Giả định partition đầu tiên là partition cần test
            if config['cluster'].get('partitions'):
                status = 'enabled' if enabled else 'disabled'
                config['cluster']['partitions'][0]['status'] = status
                self._save_config(config)
                logger.info(f"⚡ Partition Mode: {status.upper()}")
                time.sleep(2) # Chờ cluster apply config
        except Exception as e:
            logger.error(f"❌ Lỗi config partition: {e}")

    def get_leader(self, candidate_ids: Optional[List[str]] = None, timeout=10) -> Tuple[Optional[str], object]:
        """Tìm Leader trong nhóm candidate_ids (hoặc toàn bộ nếu None)"""
        start = time.time()
        while time.time() - start < timeout:
            target_nodes = [n for n in self.nodes if not candidate_ids or n['id'] in candidate_ids]
            
            for node in target_nodes:
                try:
                    client = self.pool.get_client(node['id'])
                    if client and client.get_status().get('state') == 'LEADER':
                        return node['id'], client
                except Exception:
                    pass
            time.sleep(1)
        return None, None

    def verify_data(self, node_id: str, key: str, expected_val: str, timeout=5) -> bool:
        """Kiểm tra dữ liệu trên node (có retry)"""
        start = time.time()
        while time.time() - start < timeout:
            try:
                client = self.pool.get_client(node_id)
                status = client.get_status()
                actual = status.get('state_machine', {}).get(key)
                if str(actual) == str(expected_val):
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

# --- CONTEXT MANAGER (Phần thú vị nhất) ---
@contextmanager
def NetworkPartitionContext(controller: ClusterController):
    """
    Tự động bật Partition khi vào khối 'with' 
    và Tự động tắt Partition khi thoát ra (kể cả khi lỗi).
    """
    logger.info("\n🚧 >>> BẮT ĐẦU MÔ PHỎNG SỰ CỐ MẠNG (SPLIT BRAIN) <<<")
    controller.set_partition_mode(enabled=True)
    try:
        yield # Chạy code bên trong khối with
    finally:
        logger.info("\n🚑 >>> HÀN GẮN MẠNG (AUTO HEALING) <<<")
        controller.set_partition_mode(enabled=False)
        logger.info("⏳ Chờ ổn định mạng...")
        time.sleep(3)

# --- KỊCH BẢN TEST CHÍNH ---
def sync_partition():
    cluster = ClusterController(CONFIG_PATH)
    
    # Reset trạng thái ban đầu
    cluster.set_partition_mode(enabled=False)
    
    # Tạo dữ liệu test ngẫu nhiên
    test_key = f"key_{uuid.uuid4().hex[:6]}"
    test_val = "partition_check"

    # Định nghĩa nhóm (dựa trên config YAML của bạn)
    group_majority = ['node1', 'node2', 'node3']
    victim_node = 'node5'

    logger.info("="*50)
    logger.info("🧪 BẮT ĐẦU TEST: PARTITION TOLERANCE")
    logger.info("="*50)

    # 1. BƯỚC VÀO VÙNG NGUY HIỂM (Dùng Context Manager)
    with NetworkPartitionContext(cluster):
        
        # Tìm Leader của phe đa số
        logger.info(f"🔍 Tìm Leader trong nhóm Majority {group_majority}...")
        leader_id, client = cluster.get_leader(candidate_ids=group_majority)
        
        if not client:
            logger.error("❌ Test Failed: Không bầu được Leader trong nhóm Majority.")
            return

        logger.info(f"👑 Leader Majority: {leader_id}")

        # Ghi dữ liệu khi mạng bị cắt
        logger.info(f"📝 Gửi lệnh: SET {test_key} = {test_val}")
        resp = client.client_request(f"SET {test_key} {test_val}")

        if resp and resp.get('success'):
            logger.info("✅ Ghi thành công (Quorum OK)")
        else:
            logger.error("❌ Ghi thất bại!")

        # Kiểm tra sự cô lập
        logger.info(f"🕵️ Kiểm tra {victim_node} (Kỳ vọng: KHÔNG có dữ liệu)")
        has_data = cluster.verify_data(victim_node, test_key, test_val, timeout=2)
        
        if not has_data:
            logger.info(f"✅ PASSED: {victim_node} hoàn toàn bị cô lập.")
        else:
            logger.error(f"❌ FAILED: {victim_node} vẫn nhận được dữ liệu (Lỗi Partition).")

    # 2. SAU KHI THOÁT KHỐI 'WITH', MẠNG ĐÃ TỰ ĐỘNG HÀN GẮN
    logger.info("\n🔍 Kiểm tra tính nhất quán cuối cùng (Eventual Consistency)...")
    
    # Chờ node đuổi kịp (Catch-up)
    is_synced = cluster.verify_data(victim_node, test_key, test_val, timeout=15)
    
    if is_synced:
        logger.info(f"🎉 SUCCESS: {victim_node} đã đồng bộ dữ liệu thành công!")
    else:
        logger.error(f"❌ FAILURE: {victim_node} mất dữ liệu sau khi nối mạng.")

if __name__ == "__main__":
    try:
        sync_partition()
    except KeyboardInterrupt:
        pass
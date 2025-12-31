import time
import yaml
import logging
from src.client import RaftClientPool
import uuid

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TestPartition")

CONFIG_FILE = 'config/cluster_config.yaml'

# --- CÁC HÀM HỖ TRỢ ĐỌC/GHI YAML ---

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

def toggle_partition_mode(enable: bool):
    """
    Bật/Tắt partition bằng cách sửa file YAML theo cấu trúc:
    partitions:
      - status: enabled/disabled
      - id: part1...
    """
    try:
        config = load_config()
        
        # Đảm bảo cấu trúc partitions tồn tại
        if 'partitions' not in config['cluster'] or not config['cluster']['partitions']:
            logger.error("❌ Config file thiếu mục 'partitions'.")
            return

        # Cập nhật trạng thái (phần tử đầu tiên trong list)
        status_str = 'enabled' if enable else 'disabled'
        config['cluster']['partitions'][0]['status'] = status_str
        
        save_config(config)
        logger.info(f"⚡ Đã cập nhật Partition Mode: {status_str.upper()}")
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi update config partition: {e}")

def get_leader(pool, nodes):
    """Tìm Leader hiện tại trong cluster"""
    for node in nodes:
        try:
            client = pool.get_client(node['id'])
            if client:
                status = client.get_status()
                if status and status.get('state') == 'LEADER':
                    return node['id'], client
        except Exception:
            pass
    return None, None

def check_data_on_node(pool, node_id, key, expected_value):
    """Kiểm tra dữ liệu trên một node cụ thể"""
    try:
        client = pool.get_client(node_id)
        if not client: return False, "Connect Fail"
        
        status = client.get_status()
        state_machine = status.get('state_machine', {})
        actual_value = state_machine.get(key)
        
        return str(actual_value) == str(expected_value), actual_value
    except Exception as e:
        return False, str(e)

# --- KỊCH BẢN TEST CHÍNH ---

def run_partition_test():
    logger.info("="*60)
    logger.info("TEST SCENARIO: NETWORK PARTITION (Dựa trên YAML Config)")
    logger.info("="*60)

    # 1. SETUP
    config = load_config()
    pool = RaftClientPool(config)
    nodes = config['cluster']['nodes']
    
    # Kết nối client pool
    for node in nodes:
        pool.add_node(node['id'], node['host'], node['port'])

    # Đảm bảo tắt partition trước khi bắt đầu
    logger.info("🛠️  BƯỚC 1: Đảm bảo mạng thông suốt (Disable Partition)")
    toggle_partition_mode(enable=False)
    time.sleep(3) # Chờ các node update config

    # Tìm Leader ban đầu
    leader_id, leader_client = get_leader(pool, nodes)
    if not leader_id:
        logger.error("❌ Không tìm thấy Leader. Hãy đảm bảo cluster đang chạy.")
        return
    logger.info(f"👑 Leader ban đầu: {leader_id}")

    # Ghi dữ liệu kiểm thử
    logger.info("📝 Ghi dữ liệu 'init_key' = '1'")
    leader_client.client_request("SET init_key 1")
    time.sleep(2)

    # 2. BẬT PARTITION (CẮT MẠNG)
    logger.info("\n🚧 BƯỚC 2: KÍCH HOẠT PARTITION (Split Brain)")
    toggle_partition_mode(enable=True)
    
    # Chờ config cập nhật
    time.sleep(2) 

    # Xác định danh sách node trong nhóm Majority
    majority_nodes = ['node1', 'node2', 'node3']
    
    logger.info("🔍 Tìm Leader trong phân vùng Majority (Group 1)...")
    
    # --- [FIX] THÊM VÒNG LẶP RETRY TÌM LEADER ---
    maj_leader = None
    maj_client = None
    
    # Thử tìm trong 10 giây (vì bầu cử có thể mất 2-3s)
    for i in range(10):
        # Chỉ tìm trong danh sách node thuộc Majority
        candidates = [n for n in nodes if n['id'] in majority_nodes]
        maj_leader, maj_client = get_leader(pool, candidates)
        
        if maj_leader:
            break
            
        logger.info(f"   ... đang chờ bầu cử lại (Attempt {i+1}/10) ...")
        time.sleep(1.0)
    # ---------------------------------------------
    
    if not maj_leader:
        logger.error("❌ Mất Leader trong nhóm Majority! Test thất bại (Quá thời gian bầu cử).")
        return
    
    logger.info(f"👑 Leader của nhóm Majority: {maj_leader}")

    # 3. TEST GHI KHI BỊ CHIA CẮT
    random_id = str(uuid.uuid4())[:8]
    test_key = f"key_{random_id}"
    test_val = f"val_{random_id}"
    
    logger.info(f"\n📝 BƯỚC 3: Gửi lệnh SET {test_key} = {test_val} tới Leader {maj_leader}")
    resp = maj_client.client_request(f"SET {test_key} {test_val}")
    
    if resp and resp.get('success'):
        logger.info("✅ Ghi THÀNH CÔNG (Đúng kỳ vọng: Quorum 3/5 node vẫn thông nhau)")
    else:
        logger.error(f"❌ Ghi THẤT BẠI. Lỗi: {resp}")

    # 4. KIỂM TRA NODE BỊ CÔ LẬP
    logger.info("\n🕵️  BƯỚC 4: Kiểm tra node bị cô lập (node4/node5)")
    victim_node = 'node5' 
    is_synced, val = check_data_on_node(pool, victim_node, test_key, test_val)
    
    if not is_synced:
        logger.info(f"✅ Node {victim_node} KHÔNG có dữ liệu mới. (Giá trị: {val}) -> Partition hoạt động tốt!")
    else:
        logger.error(f"❌ LỖI: Node {victim_node} đã nhận được dữ liệu! Partition config không chặn được RPC.")

    # 5. HÀN GẮN (HEAL)
    logger.info("\n🚑 BƯỚC 5: HÀN GẮN MẠNG (Disable Partition)")
    toggle_partition_mode(enable=False)
    
    logger.info("⏳ Đang chờ đồng bộ dữ liệu (Catch-up)... (Chờ 15s)")
    time.sleep(15) 
    
    # 6. VERIFY FINAL
    logger.info("\n🔍 BƯỚC 6: Kiểm tra lại tính nhất quán dữ liệu")
    is_synced, val = check_data_on_node(pool, victim_node, test_key, test_val)
    
    if is_synced:
        logger.info(f"🎉 THÀNH CÔNG: Node {victim_node} đã đồng bộ được '{test_key}'='{val}'")
    else:
        logger.error(f"❌ THẤT BẠI: Node {victim_node} vẫn chưa có dữ liệu. Giá trị: {val}")

if __name__ == "__main__":
    try:
        run_partition_test()
    except KeyboardInterrupt:
        logger.info("Test stopped.")
import time
import yaml
import logging
from src.client import RaftClientPool

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("TestFailure")

CONFIG_FILE = 'config/cluster_config.yaml'

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

def update_node_status(node_id, status):
    """Cập nhật trạng thái node trong file YAML"""
    try:
        config = load_config()
        for node in config['cluster']['nodes']:
            if node['id'] == node_id:
                node['status'] = status
        
        with open(CONFIG_FILE, 'w') as f:
            yaml.dump(config, f)
        logger.info(f"⚡ Đã cập nhật trạng thái {node_id} thành {status}")
    except Exception as e:
        logger.error(f"Lỗi khi update config: {e}")

def wait_for_leader(pool, nodes, timeout=15):
    """[MỚI] Thử tìm Leader trong vòng timeout giây"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        for node in nodes:
            try:
                client = pool.get_client(node['id'])
                if client:
                    status = client.get_status()
                    if status and status.get('state') == 'LEADER':
                        return node['id'], client
            except Exception:
                pass # Bỏ qua lỗi kết nối tạm thời
        time.sleep(1)
        logger.info("... Đang tìm Leader ...")
    
    return None, None

def wait_for_sync(client, key, expected_value, timeout=15):
    """[MỚI] Chờ dữ liệu được đồng bộ về node"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            status = client.get_status()
            current_val = status.get('state_machine', {}).get(key)
            if str(current_val) == str(expected_value):
                return True, status
        except Exception:
            pass
        time.sleep(1)
    return False, client.get_status()

def run_sync_test():
    logger.info("="*50)
    logger.info("TEST CASE 1: ĐỒNG BỘ HÓA DỮ LIỆU (LOG CATCH-UP)")
    logger.info("="*50)

    config = load_config()
    pool = RaftClientPool(config)
    nodes = config['cluster']['nodes']
    
    # 1. Setup kết nối
    for node in nodes:
        pool.add_node(node['id'], node['host'], node['port'])
        update_node_status(node['id'], 'up')
    
    # Chờ cluster ổn định và bầu Leader
    logger.info("⏳ Đang chờ bầu cử Leader...")
    leader_id, leader_client = wait_for_leader(pool, nodes)
    
    if not leader_id:
        logger.error("❌ Không tìm thấy Leader! Kiểm tra lại server.")
        return

    logger.info(f"👑 Leader hiện tại: {leader_id}")

    # 2. Ghi dữ liệu ban đầu
    logger.info("📝 Gửi Command 1: SET x 100")
    leader_client.client_request("SET x 100")
    time.sleep(1)

    # 3. Chọn Follower để 'giết'
    target_node = [n['id'] for n in nodes if n['id'] != leader_id][0]
    logger.info(f"💀 Giết node follower: {target_node}")
    update_node_status(target_node, 'down')
    time.sleep(2)

    # 4. Ghi dữ liệu mới khi target_node chết
    logger.info("📝 Gửi Command 2 & 3 (khi node kia chết)")
    leader_client.client_request("SET y 200")
    leader_client.client_request("SET z 300")
    
    # 5. Hồi sinh node
    logger.info(f"🚑 Hồi sinh node: {target_node}")
    update_node_status(target_node, 'up')
    
    # [FIX] Sử dụng hàm chờ thông minh thay vì sleep cứng
    logger.info(f"⏳ Đang chờ node {target_node} đồng bộ dữ liệu (Max 15s)...")
    target_client = pool.get_client(target_node)
    
    synced, status = wait_for_sync(target_client, 'z', '300')

    logger.info(f"🔍 Trạng thái cuối cùng của {target_node}:")
    logger.info(f"   - Log Length: {status.get('log_length')}")
    logger.info(f"   - Data: {status.get('state_machine')}")

    if synced:
        logger.info("✅ KẾT QUẢ: Node đã đồng bộ thành công!")
    else:
        logger.error("❌ KẾT QUẢ: Node KHÔNG đồng bộ được dữ liệu.")

def run_threshold_test():
    logger.info("\n" + "="*50)
    logger.info("TEST CASE 2: NGƯỠNG THẤT BẠI (FAILURE THRESHOLD)")
    logger.info("="*50)
    
    config = load_config()
    pool = RaftClientPool(config)
    nodes = config['cluster']['nodes']
    
    # Init pool
    for node in nodes:
        pool.add_node(node['id'], node['host'], node['port'])

    # Reset cluster
    logger.info("🔄 Reset trạng thái cluster...")
    for node in nodes:
        update_node_status(node['id'], 'up')
    
    logger.info("⏳ Chờ 5s để cluster ổn định...")
    time.sleep(5) 
    
    total_nodes = len(nodes)
    quorum = (total_nodes // 2) + 1
    logger.info(f"📊 Cluster size: {total_nodes}, Quorum cần thiết: {quorum}")
    
    dead_count = 0
    
    # Vòng lặp test giết dần từng node
    for i in range(total_nodes):
        # Load config mới nhất để kiểm tra trạng thái
        current_config = load_config()
        current_nodes_status = current_config['cluster']['nodes']
        alive_nodes = total_nodes - dead_count
        
        leader_id = None
        leader_client = None

        # [FIX] Vòng lặp tìm Leader thông minh (Smart Retry)
        # Nếu tìm thấy Leader là một node ĐANG DOWN, nghĩa là cluster chưa bầu xong -> Chờ tiếp
        for attempt in range(6): # Thử tối đa 6 lần (khoảng 12-15s)
            temp_id, temp_client = wait_for_leader(pool, nodes, timeout=3)
            
            # Kiểm tra xem Leader tìm được có đang 'up' không?
            is_leader_alive = False
            for n in current_nodes_status:
                if n['id'] == temp_id and n['status'] == 'up':
                    is_leader_alive = True
                    break
            
            if is_leader_alive and temp_client:
                leader_id = temp_id
                leader_client = temp_client
                break # Tìm thấy Leader hợp lệ
            else:
                if temp_id:
                    logger.warning(f"   ⚠️ Cluster báo {temp_id} là Leader nhưng node này đang DOWN. Chờ bầu cử lại... ({attempt+1}/6)")
                else:
                    logger.info(f"   ... Đang chờ bầu cử ... ({attempt+1}/6)")
                time.sleep(2)
                # Load lại config phòng trường hợp trạng thái thay đổi
                current_config = load_config()
                current_nodes_status = current_config['cluster']['nodes']

        # Kết thúc tìm Leader, bắt đầu Test
        if not leader_client:
            if alive_nodes >= quorum:
                logger.error(f"❌ Vẫn còn {alive_nodes} node nhưng không bầu được Leader hợp lệ.")
            else:
                logger.info(f"✅ Không tìm thấy Leader (Đúng dự kiến vì mất Quorum: {alive_nodes} < {quorum})")
            
            # Nếu mất Quorum thì thôi không cần gửi lệnh nữa, nhưng vẫn phải giết tiếp để test logic
            if alive_nodes >= quorum:
                break 

        # Nếu còn đủ Quorum thì thử ghi dữ liệu
        if alive_nodes >= quorum and leader_client:
            cmd = f"SET check_{alive_nodes} ok"
            logger.info(f"   👉 [Alive: {alive_nodes}] Gửi lệnh tới Leader {leader_id}: {cmd}")
            
            try:
                response = leader_client.client_request(cmd)
                is_success = response and response.get('success')
                
                if is_success:
                    logger.info(f"   ✅ Ghi thành công (Đủ Quorum)")
                else:
                    # Đọc lỗi trả về
                    err = response.get('error') if response else "No response"
                    logger.warning(f"   ⚠️ Thất bại dù đủ Quorum. Lỗi: {err}")

            except Exception as e:
                logger.info(f"   ℹ️ Lỗi kết nối: {e}")
        
        elif alive_nodes < quorum:
             # Logic kiểm tra khi mất Quorum (đã handle ở trên hoặc check nhanh)
             pass

        # 4. Chọn nạn nhân tiếp theo
        victim_id = None
        # Ưu tiên giết Follower trước, để dành Leader giết sau cùng (để test sự ổn định)
        # Nhưng nếu muốn test bầu cử, ta giết Leader luôn cũng được.
        # Logic cũ của bạn: Giết bất kỳ ai khác Leader.
        
        # Để test chặt chẽ: Ta sẽ giết chính Leader hiện tại nếu có thể, hoặc random node đang UP
        target_list = [n['id'] for n in current_nodes_status if n['status'] == 'up']
        
        if not target_list:
            break # Hết node để giết
            
        # Chiến thuật: Nếu Leader còn sống, giết Leader để ép bầu cử lại (Test Hardcore hơn)
        if leader_id in target_list:
            victim_id = leader_id
        else:
            victim_id = target_list[0]

        logger.info(f"🔻 Tắt node {victim_id}...")
        update_node_status(victim_id, 'down')
        dead_count += 1
        time.sleep(3) # Chờ status cập nhật

    # Restore
    logger.info("🔄 Khôi phục lại toàn bộ cluster...")
    for n in nodes:
        update_node_status(n['id'], 'up')

if __name__ == "__main__":
    try:
        run_sync_test()
        time.sleep(2)
        run_threshold_test()
    except KeyboardInterrupt:
        logger.info("Test stopped.")
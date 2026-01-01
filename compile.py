
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
        project_root = Path(__file__).resolve().parent
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
        self.clean_generated_files()
        self.compile_proto()
        self.fix_imports()
        
        logger.info("Proto preparation completed!")
        return True
    
if __name__ == "__main__":
    compiler = ProtoCompiler()
    compiler.prepare()
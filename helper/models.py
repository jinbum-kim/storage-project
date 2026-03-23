from typing import Any, Optional, Callable, NamedTuple, Dict, List
import subprocess
import json
import os
from termcolor import colored

class RBDDevice(NamedTuple):
    pool: str
    name: str
    device: str
    mountpoint: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.pool}/{self.name}"

    @property
    def display_name(self) -> str:
        return f"{self.full_name} -> {self.device} -> {self.mountpoint or '(unmounted)'}"

_config_path: Optional[str] = None

def set_config_path(path: str) -> None:
    """config.json 경로를 설정합니다."""
    global _config_path
    _config_path = path

def load_favorites_config() -> Dict[str, List[str]]:
    """config.json에서 즐겨찾기 이미지 목록을 로드합니다."""
    try:
        config_path = _config_path or os.path.join(os.getcwd(), "config.json")
        if not os.path.exists(config_path):
            return {}

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config.get("favorite", {})
    except Exception as e:
        print(f"config.json 로딩 중 오류: {e}")
        return {}

class BaseExecutor:
    def __init__(self, executor: Optional[Any] = None, logger_func: Optional[Callable[[str], None]] = None):
        self.executor = executor or subprocess
        self.log = logger_func or self.print_log

    def _run(self, cmd: Any, capture_output: bool = True, text: bool = True, shell: bool = False) -> subprocess.CompletedProcess:
        return self.executor.run(
            cmd,
            capture_output=capture_output,
            text=text,
            shell=shell,
            check=False
        )
    
    def print_log(self, message: str) -> None:
        print(message + "\n")

    def log_success(self, message: str) -> None:
        self.log(colored(message, "green"))

    def log_error(self, message: str) -> None:
        self.log(colored(message, "red"))

    def log_warning(self, message: str) -> None:
        self.log(colored(message, "yellow"))

    def log_info(self, message: str) -> None:
        self.log(colored(message, "cyan")) 
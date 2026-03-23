import json
from typing import List, Union
import inquirer
from helper.models import BaseExecutor, RBDDevice

class CephRBD(BaseExecutor):
    def check_cluster(self) -> None:
        """Ceph 클러스터 접근 가능 여부를 확인합니다."""
        result = self._run(["ceph", "status"])
        if result.returncode == 0:
            self.log_success("Ceph Cluster 접근 가능")
        else:
            self.log_error("Ceph Cluster 접근 불가, 확인 필요")

    def list_images(self, pool: str) -> List[str]:
        """지정된 풀의 RBD 이미지 목록을 조회합니다."""
        result = self._run(["rbd", "ls", pool])
        if result.returncode != 0:
            self.log_error(f"풀 '{pool}'이(가) 존재하지 않거나 접근 불가")
            return []
        return [img for img in result.stdout.split() if img]

    def rbd_showmapped(self) -> str:
        """RBD 매핑 정보를 조회합니다."""
        result = self._run(["rbd", "showmapped", "--format", "json"])
        if result.returncode != 0:
            self.log_error("showmapped 명령 실행 실패")
            return "[]"
        return result.stdout

    def get_mapped_rbd_devices(self) -> List[RBDDevice]:
        """현재 매핑된 RBD 디바이스 목록을 조회합니다."""
        result = self.rbd_showmapped()

        try:
            mappings = json.loads(result)
            # 마운트 정보도 함께 조회
            mount_info = self._get_mount_info()
            
            return [
                RBDDevice(
                    pool=m.get("pool", ""),
                    name=m.get("name", ""),
                    device=m.get("device", ""),
                    mountpoint=mount_info.get(m.get("device", ""))
                ) for m in mappings
            ]
        except Exception as e:
            self.log_error(f"파싱 오류: {e}")
            return []

    def _get_mount_info(self) -> dict:
        """마운트된 디바이스 정보를 조회합니다."""
        cmd = "lsblk -ndo NAME,MOUNTPOINTS | grep '^rbd'"
        res = self._run(cmd, shell=True)
        if res.returncode not in (0, 1):
            self.log_error("lsblk 명령 실행 실패")
            return {}

        mount_info = {}
        for line in res.stdout.strip().splitlines():
            if not line.strip():
                continue
            parts = line.split(None, 1)
            dev = f"/dev/{parts[0]}"
            mount_info[dev] = parts[1] if len(parts) > 1 and parts[1].strip() else None
        return mount_info

    def map_image(self) -> None:
        """선택한 RBD 이미지를 로컬 디바이스에 매핑합니다."""
        pool = input("풀 이름 입력: ")
        images = self.list_images(pool)
        if not images:
            return

        choice = inquirer.list_input(
            message="매핑할 RBD 이미지 선택:",
            choices=images + ["취소"]
        )
        if choice == "취소":
            return

        self._map_device(pool, choice)

    def _map_device(self, pool: str, image: str) -> None:
        """디바이스를 매핑합니다."""
        dev_spec = f"{pool}/{image}"
        
        # 이미 매핑되어 있는지 확인
        mapped_devices = self.get_mapped_rbd_devices()
        for device in mapped_devices:
            if device.pool == pool and device.name == image:
                self.log_warning(f"{dev_spec}는 이미 {device.device}에 매핑되어 있습니다.")
                return
        
        # 매핑 실행
        result = self._run(["rbd", "map", dev_spec])

        if result.returncode == 0:
            self.log_success(f"{dev_spec} 매핑 완료 -> {result.stdout.strip()}")
        else:
            self.log_error(f"{dev_spec} 매핑 실패: {result.stderr.strip()}")

    def unmap_image(self) -> None:
        """선택한 RBD 디바이스의 매핑을 해제합니다."""
        devices = self.get_mapped_rbd_devices()
        if not devices:
            self.log_warning("매핑 해제할 RBD 디바이스가 없습니다.")
            return

        choices: List[tuple[str, Union[RBDDevice, int]]] = [(device.display_name, device) for device in devices]
        choices.append(("취소", -1))

        sel = inquirer.list_input(
            message="매핑 해제할 RBD 디바이스 선택:",
            choices=choices
        )
        if sel == -1:
            return

        self._unmap_device(sel)

    def _unmap_device(self, device: RBDDevice) -> None:
        """디바이스 매핑을 해제합니다."""
        result = self._run(["rbd", "unmap", device.device])
        if result.returncode == 0:
            self.log_success(f"{device.device} 매핑 해제 완료")
        else:
            self.log_error(f"{device.device} 매핑 해제 실패: {result.stderr.strip()}")

import os
from typing import List, Union
import inquirer
from models import BaseExecutor, RBDDevice
from module.ceph import CephRBD

class FsMount(BaseExecutor):
    def __init__(self, rbd: CephRBD):
        super().__init__()
        self.rbd = rbd

    def check_mount(self) -> List[str]:
        """현재 매핑된 RBD 이미지 목록을 조회합니다."""
        devices = self.rbd.get_mapped_rbd_devices()
        if not devices:
            self.log_warning("현재 조회되는 디바이스가 없습니다.")
            return []

        self.log_info("[RBD 이미지 / 디바이스노드 / 마운트포인트]")
        results = [device.display_name for device in devices]
        for result in results:
            self.log_success(result)
        
        return results

    def mount(self) -> None:
        """RBD 디바이스를 마운트합니다."""
        devices = self.rbd.get_mapped_rbd_devices()
        unmounted = [device for device in devices if not device.mountpoint]
        if not unmounted:
            self.log_warning("마운트 가능한 디바이스가 없습니다.")
            return

        choices: List[tuple[str, Union[RBDDevice, int]]] = [(device.display_name, device) for device in unmounted]
        choices.append(("취소", -1))

        sel = inquirer.list_input(
            message="마운트할 디바이스 선택 (↕:이동, Enter: 선택, Ctrl-C: 취소)",
            choices=choices
        )
        if sel == -1:
            return

        device = sel
        default_mp = f"/mnt/{device.full_name}"
        mp = input(f"마운트 경로 입력 (기본: {default_mp}): ") or default_mp
        os.makedirs(mp, exist_ok=True)

        self._mount_device(device, mp)

    def _mount_device(self, device: RBDDevice, mountpoint: str) -> None:
        """디바이스를 마운트합니다."""
        res_mount = self._run(["sudo", "mount", device.device, mountpoint])
        if res_mount.returncode == 0:
            self.log_success(f"{device.device} -> {mountpoint} 마운트 완료")
            return

        err = (res_mount.stderr or res_mount.stdout) or ""
        if "wrong fs type" in err:
            self._handle_fs_type_error(device, mountpoint)
        else:
            self.log_error(f"마운트 실패: {err.strip()}")

    def _handle_fs_type_error(self, device: RBDDevice, mountpoint: str) -> None:
        """파일시스템 타입 오류를 처리합니다."""
        self.log_warning("파일 시스템 문제 발견, mkfs.ext4 실행")
        res_fs = self._run(["sudo", "mkfs.ext4", device.device])
        if res_fs.returncode != 0:
            self.log_error(f"mkfs.ext4 실패: {res_fs.stderr.strip()}")
            return

        res_retry = self._run(["sudo", "mount", device.device, mountpoint])
        if res_retry.returncode == 0:
            self.log_success(f"{device.device} -> {mountpoint} 마운트 완료")
        else:
            self.log_error(f"마운트 재시도 실패: {res_retry.stderr.strip()}")

    def get_mounted_rbd_devices(self) -> List[RBDDevice]:
        """마운트된 RBD 디바이스 목록을 반환합니다."""
        devices = self.rbd.get_mapped_rbd_devices()
        return [device for device in devices if device.mountpoint]

    def unmount(self) -> None:
        """마운트된 디바이스를 언마운트합니다."""
        mounted = self.get_mounted_rbd_devices()
        if not mounted:
            self.log_warning("마운트 해제 가능한 디바이스가 없습니다.")
            return

        choices: List[tuple[str, Union[RBDDevice, int]]] = [(device.display_name, device) for device in mounted]
        choices.append(("취소", -1))

        sel = inquirer.list_input(
            message="마운트 해제할 디바이스 선택 (↕:이동, Enter: 선택, Ctrl-C: 취소)",
            choices=choices
        )
        if sel == -1:
            return

        device = sel
        self._unmount_device(device)

    def _unmount_device(self, device: RBDDevice) -> None:
        """디바이스를 언마운트합니다."""
        if not device.mountpoint:
            return

        res_umount = self._run(["sudo", "umount", "-l", device.mountpoint])
        if res_umount.returncode == 0:
            self.log_success(f"{device.mountpoint} 마운트 해제 완료")
        else:
            self.log_error(f"마운트 해제 실패: {res_umount.stderr.strip()}")
            return

        if self._should_unmap_device():
            self._unmap_device(device)

    def _should_unmap_device(self) -> bool:
        """디바이스 언매핑 여부를 확인합니다."""
        return inquirer.confirm("디바이스 노드도 삭제하시겠습니까?", default=False)

    def _unmap_device(self, device: RBDDevice) -> None:
        """디바이스를 언매핑합니다."""
        res_unmap = self._run(["sudo", "rbd", "unmap", device.device])
        if res_unmap.returncode == 0:
            self.log_success(f"{device.device} 매핑 해제 완료")
        else:
            self.log_error(f"매핑 해제 실패: {res_unmap.stderr.strip()}")

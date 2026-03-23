import click
import inquirer
import json
from typing import Dict, Callable, Any, List, Tuple, Union

from helper.module.ceph import CephRBD
from helper.module.fs_mount import FsMount
from helper.module.docker import Docker
from helper.module.auto import Auto
from helper.models import BaseExecutor, set_config_path

class StorageHelper(BaseExecutor):
    """스토리지 관리 도우미 클래스"""
    
    MENU_CHOICES: List[Tuple[str, Union[str, int]]] = [
        ("Ceph RBD 설정", 1),
        ("파일시스템 마운트 설정", 2),
        ("Docker 설정", 3),
        ("자동화", 4),
        ("나가기", "cancel")
    ]

    CEPH_RBD_CHOICES: List[Tuple[str, str]] = [
        ("Ceph 클러스터 접근 여부 확인", "check"),
        ("RBD 매핑 설정", "map"),
        ("RBD 매핑 해제", "unmap"),
        ("취소", "cancel")
    ]

    FS_MOUNT_CHOICES: List[Tuple[str, str]] = [
        ("마운트 설정 확인", "check"),
        ("마운트 설정", "mount"),
        ("마운트 해제", "unmount"),
        ("취소", "cancel")
    ]

    DOCKER_CHOICES: List[Tuple[str, str]] = [
        ("Docker Root Directory 확인", "check"),
        ("Docker Root Directory 변경", "change"),
        ("Docker Image 조회", "list"),
        ("Docker Image 추가", "pull"),
        ("Docker Image 삭제", "rmi"),
        ("취소", "cancel")
    ]

    AUTOMATIC_CHOICES: List[Tuple[str, str]] = [
        ("Docker Image를 RBD에 자동 배포", "deploy_images"),
        ("RBD Docker 정리 (cleanup)", "cleanup_docker"),
        ("파일시스템 초기화", "format_filesystem"),
        ("취소", "cancel")
    ]

    def __init__(self, debug: bool = False):
        """StorageHelper 초기화"""
        super().__init__()
        self.debug = debug
        self.ceph_rbd = CephRBD()
        self.fs_mount = FsMount(self.ceph_rbd)
        self.docker = Docker()
        self.auto = Auto(self.ceph_rbd, self.fs_mount, self.docker)
        self._setup_actions()

    def _setup_actions(self) -> None:
        """메뉴 액션 매핑을 설정합니다."""
        self.actions: Dict[int, Callable[[str], None]] = {
            1: self._handle_ceph_rbd,
            2: self._handle_fs_mount,
            3: self._handle_docker,
            4: self._handle_automatic
        }

    def _handle_ceph_rbd(self, action: str) -> None:
        """Ceph RBD 관련 작업을 처리합니다."""
        if action == "check":
            self.ceph_rbd.check_cluster()
        elif action == "map":
            self.ceph_rbd.map_image()
        elif action == "unmap":
            self.ceph_rbd.unmap_image()

    def _handle_fs_mount(self, action: str) -> None:
        """파일시스템 마운트 관련 작업을 처리합니다."""
        if action == "check":
            self.fs_mount.check_mount()
        elif action == "mount":
            self.fs_mount.mount()
        elif action == "unmount":
            self.fs_mount.unmount()

    def _handle_docker(self, action: str) -> None:
        """Docker 관련 작업을 처리합니다."""
        if action == "check":
            self.docker.check_docker_config()
        elif action == "change":
            self.docker.change_docker_config()
        elif action == "list":
            self.docker.list_images()
        elif action == "pull":
            self.docker.pull_image()
        elif action == "rmi":
            self.docker.rm_image()

    def _handle_automatic(self, action: str) -> None:
        """자동화 관련 작업을 처리합니다."""
        if action == "deploy_images":
            self.auto.deploy_docker_images_to_rbd()
        elif action == "cleanup_docker":
            self.auto.cleanup_docker_from_rbd()
        elif action == "format_filesystem":
            self.auto.format_filesystem()

    def _get_submenu_choices(self, menu_type: int) -> List[Tuple[str, Any]]:
        """메뉴 타입에 따른 서브메뉴 선택지를 반환합니다."""
        if menu_type == 1:
            return self.CEPH_RBD_CHOICES
        elif menu_type == 2:
            return self.FS_MOUNT_CHOICES
        elif menu_type == 3:
            return self.DOCKER_CHOICES
        elif menu_type == 4:
            return self.AUTOMATIC_CHOICES
        return []

    def run(self) -> None:
        """메인 메뉴 루프를 실행합니다."""
        while True:
            try:
                choice = inquirer.list_input(
                    message="메뉴 선택 (↕:이동, Enter: 선택)",
                    choices=self.MENU_CHOICES
                )
                
                if choice == "cancel":
                    break

                submenu_choices = self._get_submenu_choices(choice)
                if not submenu_choices:
                    continue

                submenu_action = inquirer.list_input(
                    message="세부 메뉴 선택 (↕:이동, Enter: 선택, Ctrl-C: 취소)",
                    choices=submenu_choices
                )

                if submenu_action in {"cancel", None}:
                    continue

                action = self.actions.get(choice)
                if action:
                    action(submenu_action)

            except KeyboardInterrupt:
                self.print_log("\n프로그램을 종료합니다.")
                break
            except Exception as e:
                if self.debug:
                    self.log_error(f"오류 발생: {str(e)}")
                else:
                    self.log_error("작업 처리 중 오류가 발생했습니다.")

@click.command()
@click.option("--debug", is_flag=True, help="디버그 모드 활성화")
@click.option("--config", type=click.Path(exists=True), default=None, help="config.json 경로 (기본: ./config.json)")
def storage_helper(debug: bool = False, config: str = None) -> None:
    """스토리지 관리 도우미 프로그램"""
    if config:
        set_config_path(config)
    helper = StorageHelper(debug=debug)
    helper.run()

if __name__ == "__main__":
    storage_helper()
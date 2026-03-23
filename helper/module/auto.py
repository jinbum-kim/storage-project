import json
from typing import List, Tuple
import inquirer
from helper.models import BaseExecutor, load_favorites_config
from helper.module.ceph import CephRBD
from helper.module.fs_mount import FsMount
from helper.module.docker import Docker

class Auto(BaseExecutor):
    def __init__(self, ceph_rbd: CephRBD, fs_mount: FsMount, docker: Docker):
        super().__init__()
        self.ceph_rbd = ceph_rbd
        self.fs_mount = fs_mount
        self.docker = docker

    def deploy_docker_images_to_rbd(self) -> None:
        """Docker 이미지를 RBD에 자동 배포합니다."""
        self.log_info("=== Docker 이미지 자동 배포 시작 ===")
        
        # 1. Docker 이미지 목록 입력받기
        docker_images = self._get_docker_images_input()
        if not docker_images:
            return
        
        # 2. RBD Pool 입력받기
        pool = self._get_rbd_pool_input()
        if not pool:
            return
        
        # 3. RBD 이미지 선택하기
        selected_rbd_images = self._select_rbd_images(pool)
        if not selected_rbd_images:
            return
        
        # 4. 배포 계획 확인
        if not self._confirm_deployment_plan(docker_images, pool, selected_rbd_images):
            return
        
        # 5. 실제 배포 실행
        self._execute_deployment(docker_images, pool, selected_rbd_images)

    def _get_docker_images_input(self) -> List[str]:
        """배포할 Docker 이미지 목록을 입력받습니다."""
        # 이미지 선택 방식 선택
        method = inquirer.list_input(
            message="Docker 이미지 선택 방식:",
            choices=[
                ("직접 입력", "manual"),
                ("즐겨찾기 선택", "favorites"),
                ("취소", "cancel")
            ]
        )
        
        if method == "cancel":
            self.log_info("작업이 취소되었습니다.")
            return []
        elif method == "favorites":
            return self._get_favorite_images()
        else:
            return self._get_manual_images()

    def _get_manual_images(self) -> List[str]:
        """수동으로 이미지를 입력받습니다."""
        images = []
        
        self.log_info("배포할 Docker Image 목록을 입력해주세요. (빈 값 입력 시 종료)")
        while True:
            image_name = inquirer.text(
                message=f"Docker Image 이름 입력 ({len(images)}개 입력됨, 빈 값으로 완료)"
            ).strip()
            
            if not image_name:
                break
                
            images.append(image_name)
            self.log_info(f"추가됨: {image_name}")
        
        if not images:
            self.log_warning("입력된 이미지가 없습니다.")
            return []
        
        return images

    def _get_favorite_images(self) -> List[str]:
        """즐겨찾기에서 이미지를 선택합니다."""
        favorites = load_favorites_config()
        
        if not favorites:
            self.log_warning("설정된 즐겨찾기가 없습니다.")
            return []
        
        # 즐겨찾기 카테고리 선택
        choices = [(f"{key} ({len(value)}개 이미지)", key) for key, value in favorites.items()]
        choices.append(("취소", "cancel"))
        
        selected_category = inquirer.list_input(
            message="즐겨찾기 카테고리를 선택하세요:",
            choices=choices
        )
        
        if selected_category == "cancel":
            return []
        
        selected_images = favorites[selected_category]
        
        # 선택된 이미지 목록 표시
        self.log_info(f"\n=== {selected_category} 카테고리 이미지 목록 ===")
        for i, img in enumerate(selected_images, 1):
            self.log_info(f"{i:2d}. {img}")
        
        if not inquirer.confirm(f"{selected_category} 카테고리의 {len(selected_images)}개 이미지를 배포하시겠습니까?", default=False):
            return []
        
        return selected_images

    def _get_rbd_pool_input(self) -> str:
        """RBD Pool을 입력받습니다."""
        pool = inquirer.text(message="배포할 RBD Pool 이름 입력").strip()
        if not pool:
            self.log_warning("Pool 이름이 입력되지 않았습니다.")
            return ""
        
        # Pool 존재 확인
        images = self.ceph_rbd.list_images(pool)
        if not images:
            self.log_warning(f"Pool '{pool}'이 존재하지 않거나 접근할 수 없습니다.")
            return ""
        
        self.log_success(f"Pool '{pool}' 확인됨 ({len(images)}개 이미지)")
        return pool

    def _select_rbd_images(self, pool: str) -> List[str]:
        """RBD 이미지를 효율적으로 선택합니다."""
        images = self.ceph_rbd.list_images(pool)
        if not images:
            return []
        
        # 1. 번호 매겨서 출력
        self.log_info(f"\n=== {pool} 풀의 RBD 이미지 목록 ===")
        for i, image in enumerate(images, 1):
            self.log_info(f"{i:2d}. {image}")
        
        # 2. 입력 방식 선택
        method = inquirer.list_input(
            message="선택 방식을 고르세요:",
            choices=[
                ("번호 범위 입력 (1,3-5,7 또는 all)", "range"),
                ("체크박스로 개별 선택", "checkbox"),
                ("취소", "cancel")
            ]
        )
        
        if method == "cancel":
            return []
        elif method == "range":
            return self._select_by_range(images)
        else:
            return self._select_by_checkbox(images)

    def _select_by_range(self, images: List[str]) -> List[str]:
        """범위 입력 방식으로 선택"""
        while True:
            self.log_info("입력 예시: 1,3-5,7 (1번, 3~5번, 7번 선택) 또는 all (전체 선택)")
            selection = inquirer.text(
                message="선택할 번호 입력:"
            ).strip()
            
            try:
                if selection.lower() == "all":
                    selected = images[:]
                else:
                    indices = self._parse_range(selection, len(images))
                    selected = [images[i-1] for i in indices]
                
                # 미리보기
                self.log_info(f"\n선택된 이미지 ({len(selected)}개):")
                for img in selected:
                    self.log_info(f"  ✓ {img}")
                
                if inquirer.confirm("이대로 진행하시겠습니까?", default=True):
                    return selected
                    
            except ValueError as e:
                self.log_error(f"입력 오류: {e}")
                self.log_info("올바른 형식: 1,3-5,7 또는 all")

    def _select_by_checkbox(self, images: List[str]) -> List[str]:
        """체크박스 방식으로 선택"""
        selected = inquirer.checkbox(
            message="배포할 RBD 이미지를 선택하세요 (Space: 선택/해제, Enter: 완료):",
            choices=images
        )
        return selected or []

    def _parse_range(self, selection: str, max_num: int) -> List[int]:
        """범위 문자열을 파싱합니다."""
        indices = set()
        
        for part in selection.split(','):
            part = part.strip()
            if '-' in part:
                start, end = map(int, part.split('-'))
                if start < 1 or end > max_num or start > end:
                    raise ValueError(f"잘못된 범위: {part} (1-{max_num} 범위 내에서 입력)")
                indices.update(range(start, end + 1))
            else:
                num = int(part)
                if num < 1 or num > max_num:
                    raise ValueError(f"잘못된 번호: {num} (1-{max_num} 범위 내에서 입력)")
                indices.add(num)
        
        return sorted(indices)

    def _confirm_deployment_plan(self, docker_images: List[str], pool: str, rbd_images: List[str]) -> bool:
        """배포 계획을 확인합니다."""
        self.log_info("\n=== 배포 계획 확인 ===")
        self.log_info(f"Docker 이미지 ({len(docker_images)}개):")
        for img in docker_images:
            self.log_info(f"  - {img}")
        
        self.log_info(f"\nRBD Pool: {pool}")
        self.log_info(f"대상 RBD 이미지 ({len(rbd_images)}개):")
        for img in rbd_images:
            self.log_info(f"  - {img}")
        
        self.log_warning(f"총 {len(docker_images)} × {len(rbd_images)} = {len(docker_images) * len(rbd_images)}번의 배포가 실행됩니다.")
        
        return inquirer.confirm("배포를 시작하시겠습니까?", default=False)

    def _execute_deployment(self, docker_images: List[str], pool: str, rbd_images: List[str]) -> None:
        """실제 배포를 실행합니다."""
        self.log_info("\n=== 배포 실행 시작 ===")

        total_tasks = len(docker_images) * len(rbd_images)
        current_task = 0
        success_count = 0
        failed_tasks = []
        completed_rbd_images = []
        partial_rbd_images = []

        for rbd_image in rbd_images:
            self.log_info(f"\n--- RBD 이미지: {pool}/{rbd_image} 처리 시작 ---")
            rbd_success = 0

            # RBD 매핑
            if not self._map_rbd_image(pool, rbd_image):
                self.log_error(f"RBD 매핑 실패: {pool}/{rbd_image}")
                failed_tasks.extend([(img, rbd_image, "RBD 매핑 실패") for img in docker_images])
                current_task += len(docker_images)
                continue

            # 마운트
            mounted_path = self._mount_rbd_image(pool, rbd_image)
            if not mounted_path:
                self.log_error(f"마운트 실패: {pool}/{rbd_image}")
                failed_tasks.extend([(img, rbd_image, "마운트 실패") for img in docker_images])
                current_task += len(docker_images)
                continue

            # Docker Root Directory 변경
            if not self._change_docker_root(mounted_path):
                self.log_error(f"Docker Root Directory 변경 실패: {mounted_path}")
                failed_tasks.extend([(img, rbd_image, "Docker 설정 실패") for img in docker_images])
                current_task += len(docker_images)
                continue

            # Docker 이미지들 다운로드
            for docker_image in docker_images:
                current_task += 1
                self.log_info(f"[{current_task}/{total_tasks}] {docker_image} -> {pool}/{rbd_image}")

                if self._pull_docker_image(docker_image):
                    self.log_success(f"✓ {docker_image} 다운로드 완료")
                    success_count += 1
                    rbd_success += 1
                else:
                    self.log_error(f"✗ {docker_image} 다운로드 실패")
                    failed_tasks.append((docker_image, rbd_image, "이미지 다운로드 실패"))

            if rbd_success == len(docker_images):
                completed_rbd_images.append(rbd_image)
            elif rbd_success > 0:
                partial_rbd_images.append(rbd_image)

        # 결과 요약
        self._show_deployment_summary(total_tasks, success_count, failed_tasks, pool, completed_rbd_images, partial_rbd_images)

    def _map_rbd_image(self, pool: str, image: str) -> bool:
        """RBD 이미지를 매핑합니다. 이미 매핑된 경우 스킵합니다."""
        try:
            # 이미 매핑된 디바이스 확인
            mapped_devices = self.ceph_rbd.get_mapped_rbd_devices()
            for device in mapped_devices:
                if device.pool == pool and device.name == image:
                    self.log_info(f"{pool}/{image}는 이미 {device.device}에 매핑되어 있습니다.")
                    return True
            
            # 매핑되지 않은 경우에만 매핑 실행
            self.ceph_rbd._map_device(pool, image)
            return True
        except Exception as e:
            self.log_error(f"RBD 매핑 중 오류: {e}")
            return False

    def _mount_rbd_image(self, pool: str, image: str) -> str:
        """RBD 이미지를 마운트하고 마운트 경로를 반환합니다. 이미 마운트된 경우 기존 경로를 반환합니다."""
        mount_path = f"/mnt/{pool}/{image}"
        
        try:
            # 이미 마운트된 디바이스 확인
            mapped_devices = self.ceph_rbd.get_mapped_rbd_devices()
            for device in mapped_devices:
                if device.pool == pool and device.name == image:
                    if device.mountpoint:
                        self.log_info(f"{pool}/{image}는 이미 {device.mountpoint}에 마운트되어 있습니다.")
                        return device.mountpoint
                    break
            
            # 마운트 경로 디렉토리 생성
            self._run(["sudo", "mkdir", "-p", mount_path])
            
            # RBD 디바이스 경로 찾기
            rbd_device = self._find_rbd_device(pool, image)
            if not rbd_device:
                self.log_error(f"RBD 디바이스를 찾을 수 없습니다: {pool}/{image}")
                return ""
            
            # 이미 마운트되어 있는지 재확인 (mount 명령으로)
            check_mount = self._run(["mountpoint", "-q", mount_path])
            if check_mount.returncode == 0:
                self.log_info(f"{mount_path}는 이미 마운트되어 있습니다.")
                return mount_path
            
            # 마운트 실행
            mount_result = self._run(["sudo", "mount", rbd_device, mount_path])
            if mount_result.returncode == 0:
                self.log_success(f"{rbd_device} -> {mount_path} 마운트 완료")
                return mount_path
            else:
                # 파일시스템 문제인 경우 포맷 후 재시도
                self.log_warning("파일시스템 문제 발견, mkfs.ext4 실행")
                format_result = self._run(["sudo", "mkfs.ext4", rbd_device])
                if format_result.returncode == 0:
                    retry_result = self._run(["sudo", "mount", rbd_device, mount_path])
                    if retry_result.returncode == 0:
                        self.log_success(f"{rbd_device} -> {mount_path} 마운트 완료 (포맷 후)")
                        return mount_path
                
                self.log_error(f"마운트 실패: {mount_result.stderr}")
                return ""
                
        except Exception as e:
            self.log_error(f"마운트 처리 중 오류: {e}")
            return ""

    def _find_rbd_device(self, pool: str, image: str) -> str:
        """RBD 이미지에 해당하는 디바이스 경로를 찾습니다."""
        try:
            mappings = self.ceph_rbd.rbd_showmapped()
            
            mappings = json.loads(mappings)
            for mapping in mappings:
                if mapping.get("pool") == pool and mapping.get("name") == image:
                    return mapping.get("device", "")
            
            return ""
        except Exception:
            return ""

    def _change_docker_root(self, mount_path: str) -> bool:
        """Docker Root Directory를 변경합니다. 이미 설정된 경우 스킵합니다."""
        try:
            # 현재 Docker Root Directory 확인
            current_docker_root = self.docker._check_docker_config().strip().strip("'\"")
            docker_path = f"{mount_path}/root/docker"
            
            # 이미 올바른 경로로 설정된 경우 스킵
            if current_docker_root == docker_path:
                self.log_info(f"Docker Root Directory가 이미 {docker_path}로 설정되어 있습니다.")
                return True
            
            # 설정이 다른 경우에만 변경
            self.log_info(f"Docker Root Directory를 {docker_path}로 변경합니다...")
            self.docker._change_docker_root_directory(mount_path)
            return True
        except Exception as e:
            self.log_error(f"Docker Root Directory 변경 중 오류: {e}")
            return False

    def _pull_docker_image(self, image: str) -> bool:
        """Docker 이미지를 다운로드합니다."""
        result = self._run(["docker", "pull", image], capture_output=False)
        return result.returncode == 0

    def _show_deployment_summary(self, total: int, success: int, failed_tasks: List[Tuple[str, str, str]],
                                   pool: str = "", completed: List[str] = None, partial: List[str] = None) -> None:
        """배포 결과 요약을 표시합니다."""
        self.log_info("\n=== 배포 결과 요약 ===")
        self.log_success(f"성공: {success}/{total}")

        if completed:
            self.log_success(f"완료된 RBD 이미지 ({len(completed)}개):")
            for img in completed:
                self.log_success(f"  ✓ {pool}/{img}")

        if partial:
            self.log_warning(f"부분 완료된 RBD 이미지 ({len(partial)}개) — 수동 확인 필요:")
            for img in partial:
                self.log_warning(f"  △ {pool}/{img}")

        if failed_tasks:
            self.log_error(f"실패: {len(failed_tasks)}/{total}")
            for docker_img, rbd_img, reason in failed_tasks:
                self.log_error(f"  - {docker_img} -> {rbd_img}: {reason}")

    def format_filesystem(self) -> None:
        """RBD 파일시스템을 자동 초기화합니다."""
        self.log_info("=== RBD 파일시스템 초기화 시작 ===")
        
        # 1. RBD Pool 입력받기
        pool = self._get_rbd_pool_input()
        if not pool:
            return
        
        # 2. 초기화할 RBD 이미지 범위 입력받기
        target_images = self._get_format_target_images(pool)
        if not target_images:
            return
        
        # 3. 초기화 계획 확인
        if not self._confirm_format_plan(pool, target_images):
            return
        
        # 4. 실제 초기화 실행
        self._execute_format(pool, target_images)

    def _get_format_target_images(self, pool: str) -> List[str]:
        """초기화할 RBD 이미지 목록을 입력받습니다."""
        images = self.ceph_rbd.list_images(pool)
        if not images:
            return []
        
        # 1. 번호 매겨서 출력
        self.log_info(f"\n=== {pool} 풀의 RBD 이미지 목록 ===")
        for i, image in enumerate(images, 1):
            self.log_info(f"{i:2d}. {image}")
        
        # 2. 초기화 방식 선택
        method = inquirer.list_input(
            message="초기화할 RBD 이미지 선택 방식:",
            choices=[
                ("범위 입력 (예: 1-10, 1,3,5)", "range"),
                ("체크박스로 개별 선택", "checkbox"),
                ("전체 선택", "all"),
                ("취소", "cancel")
            ]
        )
        
        if method == "cancel":
            return []
        elif method == "all":
            return self._confirm_all_format(images)
        elif method == "range":
            return self._select_format_by_range(images)
        else:
            return self._select_format_by_checkbox(images)

    def _confirm_all_format(self, images: List[str]) -> List[str]:
        """전체 이미지 초기화 확인"""
        self.log_warning(f"⚠️  주의: 전체 {len(images)}개 이미지가 초기화됩니다!")
        for img in images:
            self.log_warning(f"  - {img}")
        
        if inquirer.confirm("⚠️  모든 데이터가 삭제됩니다. 정말 계속하시겠습니까?", default=False):
            return images
        return []

    def _select_format_by_range(self, images: List[str]) -> List[str]:
        """범위 입력으로 초기화 대상 선택"""
        while True:
            self.log_info("입력 예시: 1-10 (1~10번), 1,3,5 (1,3,5번), 1-5,8,10-12 (혼합)")
            selection = inquirer.text(
                message="초기화할 이미지 번호 입력:"
            ).strip()
            
            try:
                indices = self._parse_range(selection, len(images))
                selected = [images[i-1] for i in indices]
                
                # 미리보기
                self.log_warning(f"\n⚠️  초기화될 이미지 ({len(selected)}개):")
                for img in selected:
                    self.log_warning(f"  - {img}")
                
                if inquirer.confirm("⚠️  위 이미지들의 모든 데이터가 삭제됩니다. 계속하시겠습니까?", default=False):
                    return selected
                    
            except ValueError as e:
                self.log_error(f"입력 오류: {e}")
                self.log_info("올바른 형식: 1-10, 1,3,5 등")

    def _select_format_by_checkbox(self, images: List[str]) -> List[str]:
        """체크박스로 초기화 대상 선택"""
        selected = inquirer.checkbox(
            message="초기화할 RBD 이미지를 선택하세요 (Space: 선택/해제, Enter: 완료):",
            choices=images
        )
        
        if not selected:
            return []
        
        # 확인
        self.log_warning(f"\n⚠️  초기화될 이미지 ({len(selected)}개):")
        for img in selected:
            self.log_warning(f"  - {img}")
        
        if inquirer.confirm("⚠️  위 이미지들의 모든 데이터가 삭제됩니다. 계속하시겠습니까?", default=False):
            return selected
        return []

    def _confirm_format_plan(self, pool: str, target_images: List[str]) -> bool:
        """초기화 계획을 확인합니다."""
        self.log_warning("\n=== ⚠️  파일시스템 초기화 계획 ===")
        self.log_warning(f"Pool: {pool}")
        self.log_warning(f"초기화 대상 ({len(target_images)}개 이미지):")
        for img in target_images:
            self.log_warning(f"  - {img}")
        
        self.log_error("⚠️  경고: 모든 데이터가 영구적으로 삭제됩니다!")
        self.log_error("⚠️  이 작업은 되돌릴 수 없습니다!")
        
        return inquirer.confirm("정말로 초기화를 진행하시겠습니까?", default=False)

    def _execute_format(self, pool: str, target_images: List[str]) -> None:
        """실제 파일시스템 초기화를 실행합니다."""
        self.log_info("\n=== 파일시스템 초기화 실행 ===")
        
        success_count = 0
        failed_images = []
        
        for i, image in enumerate(target_images, 1):
            self.log_info(f"[{i}/{len(target_images)}] {pool}/{image} 초기화 중...")
            
            # RBD 디바이스 경로 찾기
            device_path = self._find_rbd_device(pool, image)
            
            # 디바이스가 없으면 RBD 매핑 시도
            if not device_path:
                self.log_warning(f"  {pool}/{image}: 디바이스가 매핑되지 않음, 매핑 시도 중...")
                
                # RBD 매핑 시도
                if self._map_rbd_image(pool, image):
                    # 매핑 성공 후 다시 디바이스 찾기
                    device_path = self._find_rbd_device(pool, image)
                    if device_path:
                        self.log_success(f"  {pool}/{image}: 매핑 완료 -> {device_path}")
                    else:
                        self.log_error(f"✗ {pool}/{image}: 매핑 후에도 디바이스를 찾을 수 없습니다")
                        failed_images.append((image, "매핑 후 디바이스 없음"))
                        continue
                else:
                    self.log_error(f"✗ {pool}/{image}: RBD 매핑 실패")
                    failed_images.append((image, "RBD 매핑 실패"))
                    continue
            
            # mkfs.ext4 -F 실행
            self.log_info(f"  디바이스: {device_path}")
            result = self._run(["sudo", "mkfs.ext4", "-F", device_path])
            
            if result.returncode == 0:
                self.log_success(f"✓ {pool}/{image} ({device_path}) 초기화 완료")
                success_count += 1
            else:
                error_msg = result.stderr.strip() if result.stderr else "알 수 없는 오류"
                self.log_error(f"✗ {pool}/{image} ({device_path}) 초기화 실패: {error_msg}")
                failed_images.append((image, error_msg))
        
        # 결과 요약
        self._show_format_summary(len(target_images), success_count, failed_images)

    def _show_format_summary(self, total: int, success: int, failed_images: List[Tuple[str, str]]) -> None:
        """초기화 결과 요약을 표시합니다."""
        self.log_info("\n=== 파일시스템 초기화 결과 ===")
        self.log_success(f"성공: {success}/{total}")
        
        if failed_images:
            self.log_error(f"실패: {len(failed_images)}/{total}")
            for image, reason in failed_images:
                self.log_error(f"  - {image}: {reason}")
        
        if success > 0:
            self.log_info("초기화된 이미지를 마운트하려면 '파일시스템 마운트 설정' 메뉴를 사용하세요.")

    def cleanup_docker_from_rbd(self) -> None:
        """RBD 이미지 내 Docker 데이터를 정리합니다."""
        self.log_info("=== RBD Docker 정리 (cleanup) 시작 ===")

        # 1. RBD Pool 입력받기
        pool = self._get_rbd_pool_input()
        if not pool:
            return

        # 2. 정리할 RBD 이미지 선택
        selected_rbd_images = self._select_rbd_images(pool)
        if not selected_rbd_images:
            return

        # 3. 정리 계획 확인
        if not self._confirm_cleanup_plan(pool, selected_rbd_images):
            return

        # 4. 실제 정리 실행
        self._execute_cleanup(pool, selected_rbd_images)

    def _confirm_cleanup_plan(self, pool: str, rbd_images: List[str]) -> bool:
        """정리 계획을 확인합니다."""
        self.log_warning("\n=== ⚠️  Docker 정리 계획 ===")
        self.log_warning(f"Pool: {pool}")
        self.log_warning(f"대상 RBD 이미지 ({len(rbd_images)}개):")
        for img in rbd_images:
            self.log_warning(f"  - {img}  →  /mnt/{pool}/{img}/root/docker 삭제")

        self.log_error("⚠️  경고: 대상 RBD 이미지의 Docker 데이터가 영구적으로 삭제됩니다!")

        return inquirer.confirm("정말로 정리를 진행하시겠습니까?", default=False)

    def _execute_cleanup(self, pool: str, rbd_images: List[str]) -> None:
        """실제 Docker 정리를 실행합니다."""
        self.log_info("\n=== Docker 정리 실행 ===")

        success_count = 0
        failed_images = []

        for i, image in enumerate(rbd_images, 1):
            self.log_info(f"[{i}/{len(rbd_images)}] {pool}/{image} 정리 중...")

            # 1. RBD 매핑
            if not self._map_rbd_image(pool, image):
                failed_images.append((image, "RBD 매핑 실패"))
                continue

            # 2. 마운트
            mounted_path = self._mount_rbd_image(pool, image)
            if not mounted_path:
                failed_images.append((image, "마운트 실패"))
                continue

            # 3. Docker root 삭제
            docker_root = f"{mounted_path}/root/docker"
            result = self._run(["sudo", "rm", "-rf", docker_root])
            if result.returncode != 0:
                failed_images.append((image, f"Docker 디렉토리 삭제 실패"))
                continue

            self.log_success(f"  {docker_root} 삭제 완료")

            # 4. 언마운트
            umount_result = self._run(["sudo", "umount", "-l", mounted_path])
            if umount_result.returncode != 0:
                self.log_warning(f"  {mounted_path} 마운트 해제 실패")
                failed_images.append((image, "마운트 해제 실패"))
                continue

            self.log_success(f"  {mounted_path} 마운트 해제 완료")

            # 5. 언맵
            device_path = self._find_rbd_device(pool, image)
            if device_path:
                unmap_result = self._run(["sudo", "rbd", "unmap", device_path])
                if unmap_result.returncode != 0:
                    self.log_warning(f"  {device_path} 매핑 해제 실패")
                    failed_images.append((image, "매핑 해제 실패"))
                    continue
                self.log_success(f"  {device_path} 매핑 해제 완료")

            success_count += 1

        self._show_cleanup_summary(len(rbd_images), success_count, failed_images)

    def _show_cleanup_summary(self, total: int, success: int, failed_images: List[Tuple[str, str]]) -> None:
        """정리 결과 요약을 표시합니다."""
        self.log_info("\n=== Docker 정리 결과 ===")
        self.log_success(f"성공: {success}/{total}")

        if failed_images:
            self.log_error(f"실패: {len(failed_images)}/{total}")
            for image, reason in failed_images:
                self.log_error(f"  - {image}: {reason}")

from helper.models import BaseExecutor, load_favorites_config
from helper.module.fs_mount import FsMount
import inquirer

class Docker(BaseExecutor):
    def _check_docker_config(self) -> str:
        """Docker Root Directory 확인"""
        # docker info --format '{{.DockerRootDir}}'
        res = self._run(["docker", "info", "--format", "'{{.DockerRootDir}}'"])
        if res.returncode != 0:
            self.log_error("Docker Root Directory 확인 실패")
            return ""
        
        return res.stdout
        
    def check_docker_config(self) -> None:
        """현재 Docker Root Directory 확인"""
        self.log_info("현재 Docker Root Directory: " + self._check_docker_config())

    def change_docker_config(self) -> None:
        """Docker Root Directory 변경"""
        # 시작 전 안내 메시지
        self.log_warning("Docker Root Directory 변경을 시작합니다.")
        self.log_warning("변경 전 모든 Docker 컨테이너를 중지해주세요.")
        
        if not inquirer.confirm("계속 진행하시겠습니까?", default=False):
            self.log_info("작업이 취소되었습니다.")
            return

        # 1. 현재 Docker Root Directory 확인 후 출력
        current_docker_root = self._check_docker_config()
        self.log_info(f"현재 Docker Root Directory: {current_docker_root}")

        # 2. 현재 마운트된 RBD 디바이스 목록 조회 및 선택
        from helper.module.ceph import CephRBD
        ceph_rbd = CephRBD()
        fs_mount = FsMount(ceph_rbd)
        mounted_devices = fs_mount.get_mounted_rbd_devices()
        
        self.log_info("Docker Root Directory 선택 옵션:")
        choices = [("기본 경로 (/var/lib/docker)", "/var/lib/docker")]
        
        if mounted_devices:
            choices.extend([(device.display_name, device.mountpoint or "") for device in mounted_devices])
        else:
            self.log_warning("마운트된 RBD 디바이스가 없습니다.")
        
        choices.append(("취소", "cancel"))

        selected_mountpoint = inquirer.list_input(
            message="Docker Root Directory로 사용할 경로 선택 (↕:이동, Enter: 선택, Ctrl-C: 취소)",
            choices=choices
        )
        
        if selected_mountpoint == "cancel":
            self.log_info("작업이 취소되었습니다.")
            return

        # 3. Docker Root Directory 변경 로직 구현
        self._change_docker_root_directory(selected_mountpoint)
        
        # 4. 변경 후 확인
        new_docker_root = self._check_docker_config()
        self.log_success(f"Docker Root Directory가 변경되었습니다: {new_docker_root}")

    def _change_docker_root_directory(self, mountpoint: str) -> None:
        """Docker Root Directory를 변경합니다."""
        service_file = "/lib/systemd/system/docker.service"
        
        try:
            # Docker 서비스 중지
            self.log_info("Docker 서비스를 중지합니다...")
            self._run(["sudo", "systemctl", "stop", "docker.socket", "docker.service"])
            
            # 기본 경로(/var/lib/docker)로 복원하는 경우
            if mountpoint == "/var/lib/docker":
                self.log_info("기본 Docker Root Directory로 복원합니다...")
                self._restore_default_docker_config(service_file)
            else:
                # 새로운 경로로 변경하는 경우
                docker_path = f"{mountpoint}/root/docker"
                
                # 디렉토리 생성
                self.log_info(f"Docker 디렉토리를 생성합니다: {docker_path}")
                self._run(["sudo", "mkdir", "-p", docker_path])
                
                # systemd 설정 파일 백업 및 수정
                new_exec_start = f"ExecStart=/usr/bin/dockerd --data-root {docker_path} -H fd://"
                
                self.log_info("Docker 서비스 설정을 변경합니다...")
                # 최초 원본 백업이 없을 때만 .bak 생성 (덮어쓰기 방지)
                check_bak = self._run(["sudo", "test", "-f", f"{service_file}.bak"])
                if check_bak.returncode != 0:
                    self._run(["sudo", "cp", service_file, f"{service_file}.bak"])
                
                # Python으로 파일 직접 수정
                self._modify_docker_service_file(service_file, new_exec_start)
            
            # systemd 재로드 및 Docker 서비스 시작
            self.log_info("systemd를 재로드하고 Docker 서비스를 시작합니다...")
            self._run(["sudo", "systemctl", "daemon-reload"])
            self._run(["sudo", "systemctl", "start", "docker"])
            
            self.log_success("Docker Root Directory 변경이 완료되었습니다.")
            
        except Exception as e:
            self.log_error(f"Docker Root Directory 변경 중 오류 발생: {e}")
            self.log_info("원본 설정으로 복구를 시도합니다...")
            try:
                self._run(["sudo", "cp", f"{service_file}.bak", service_file])
                self._run(["sudo", "systemctl", "daemon-reload"])
                self._run(["sudo", "systemctl", "start", "docker"])
                self.log_info("원본 설정으로 복구되었습니다.")
            except:
                self.log_error("복구 실패. 수동으로 Docker 설정을 확인해주세요.")

    def _restore_default_docker_config(self, service_file: str) -> None:
        """기본 Docker 설정으로 복원합니다."""
        backup_file = f"{service_file}.bak"
        
        # 백업 파일이 존재하는지 확인
        check_backup = self._run(["sudo", "test", "-f", backup_file])
        if check_backup.returncode == 0:
            # 백업 파일이 존재하면 복원
            self.log_info("백업 파일을 이용해 기본 설정으로 복원합니다...")
            self._run(["sudo", "rm", "-f", service_file])
            self._run(["sudo", "mv", backup_file, service_file])
            self.log_success("기본 Docker 설정으로 복원 완료")
        else:
            # 백업 파일이 없으면 기본 ExecStart로 수정
            self.log_warning("백업 파일이 없어 기본 ExecStart로 직접 수정합니다...")
            default_exec_start = "ExecStart=/usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock"
            self._modify_docker_service_file(service_file, default_exec_start)
            self.log_success("기본 ExecStart로 수정 완료")

    def _modify_docker_service_file(self, service_file: str, new_exec_start: str) -> None:
        """Docker 서비스 파일을 수정합니다."""
        import tempfile
        import os
        
        try:
            # 임시 파일 생성
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.service') as temp_file:
                temp_path = temp_file.name
                
                # 원본 파일 읽기
                read_result = self._run(["sudo", "cat", service_file])
                if read_result.returncode != 0:
                    raise Exception(f"원본 파일 읽기 실패: {read_result.stderr}")
                
                # 파일 내용 수정
                lines = read_result.stdout.splitlines()
                modified_lines = []
                
                for line in lines:
                    if line.strip().startswith("ExecStart="):
                        modified_lines.append(new_exec_start)
                        self.log_info(f"ExecStart 라인 변경: {line.strip()} -> {new_exec_start}")
                    else:
                        modified_lines.append(line)
                
                # 임시 파일에 수정된 내용 쓰기
                temp_file.write('\n'.join(modified_lines) + '\n')
            
            # 임시 파일을 원본 위치로 복사
            copy_result = self._run(["sudo", "cp", temp_path, service_file])
            if copy_result.returncode != 0:
                raise Exception(f"파일 복사 실패: {copy_result.stderr}")
            
            # 권한 설정
            self._run(["sudo", "chmod", "644", service_file])
            
            # 임시 파일 삭제
            os.unlink(temp_path)
            
            self.log_success("Docker 서비스 파일 수정 완료")
            
            # 수정 결과 확인
            verify_result = self._run(["sudo", "grep", "^ExecStart=", service_file])
            if verify_result.returncode == 0:
                self.log_info(f"수정 확인: {verify_result.stdout.strip()}")
            
        except Exception as e:
            self.log_error(f"Docker 서비스 파일 수정 실패: {e}")
            # 임시 파일이 있다면 삭제
            try:
                if 'temp_path' in locals():
                    os.unlink(temp_path)
            except:
                pass
            raise

    def list_images(self) -> None:
        """Docker Image 목록 조회"""
        res = self._run(["docker", "images"])
        if res.returncode != 0:
            self.log_error("Docker Image 목록 조회 실패")
            return
        
        self.print_log(res.stdout)

    def pull_image(self) -> None:
        """Docker Image 다운로드"""
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
            self.log_info("다운로드가 취소되었습니다.")
            return
        elif method == "favorites":
            images = self._get_favorite_images()
        else:
            images = self._get_manual_images()
        
        if not images:
            self.log_warning("입력된 이미지가 없습니다.")
            return
        
        self.log_info(f"총 {len(images)}개의 이미지를 다운로드합니다:")
        for img in images:
            self.log_info(f"  - {img}")
        
        if not inquirer.confirm("다운로드를 시작하시겠습니까?", default=False):
            self.log_info("다운로드가 취소되었습니다.")
            return
        
        # 이미지를 차례대로 다운로드
        success_count = 0
        failed_images = []
        
        for i, image_name in enumerate(images, 1):
            self.log_info(f"[{i}/{len(images)}] {image_name} 다운로드 중...")
            
            res = self._run(["docker", "pull", image_name], capture_output=False)
            if res.returncode == 0:
                self.log_success(f"✓ {image_name} 다운로드 완료")
                success_count += 1
            else:
                self.log_error(f"✗ {image_name} 다운로드 실패")
                failed_images.append(image_name)
        
        # 결과 요약
        self.log_info(f"\n=== 다운로드 결과 ===")
        self.log_success(f"성공: {success_count}개")
        if failed_images:
            self.log_error(f"실패: {len(failed_images)}개")
            for failed_img in failed_images:
                self.log_error(f"  - {failed_img}")

    def _get_manual_images(self) -> list:
        """수동으로 이미지를 입력받습니다."""
        images = []
        
        self.log_info("다운로드할 Docker Image 목록을 입력해주세요. (빈 값 입력 시 종료)")
        while True:
            image_name = inquirer.text(
                message=f"Docker Image 이름 입력 ({len(images)}개 입력됨, 빈 값으로 완료)"
            ).strip()
            
            if not image_name:
                break
                
            images.append(image_name)
            self.log_info(f"추가됨: {image_name}")
        
        return images

    def _get_favorite_images(self) -> list:
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
        
        if not inquirer.confirm(f"{selected_category} 카테고리의 {len(selected_images)}개 이미지를 다운로드하시겠습니까?", default=False):
            return []
        
        return selected_images

    def rm_image(self) -> None:
        """Docker Image 삭제"""
        res = self._run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.Size}}"])
        if res.returncode != 0:
            self.log_error("Docker Image 목록 조회 실패")
            return

        lines = [line for line in res.stdout.strip().splitlines() if line.strip()]
        if not lines:
            self.log_warning("삭제할 Docker Image가 없습니다.")
            return

        image_choices = []
        for line in lines:
            parts = line.split("\t")
            name = parts[0]
            size = parts[2] if len(parts) > 2 else ""
            label = f"{name}  ({size})" if size else name
            image_choices.append((label, name))

        image_choices.append(("취소", "cancel"))

        selected = inquirer.list_input(
            message="삭제할 Docker Image 선택:",
            choices=image_choices
        )
        if selected == "cancel":
            return

        if not inquirer.confirm(f"{selected} 이미지를 삭제하시겠습니까?", default=False):
            self.log_info("삭제가 취소되었습니다.")
            return

        res = self._run(["docker", "rmi", selected])
        if res.returncode != 0:
            self.log_error("Docker Image 삭제 실패")
            return

        self.log_success(f"{selected} 삭제 완료")
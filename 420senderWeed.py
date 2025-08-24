import os
import sys
import json
import time
import logging
import hashlib
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MediaUploaderConfig:
    def __init__(self, config_path: str = 'config.yaml'):
        """โหลดการตั้งค่าจากไฟล์ YAML"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            logging.error(f"ไม่พบไฟล์การตั้งค่า: {config_path}")
            self.config = {}

    def get(self, key: str, default=None):
        """ดึงค่าการตั้งค่า"""
        return self.config.get(key, default)

class MediaUploader:
    def __init__(self, config: MediaUploaderConfig):
        """เริ่มต้นระบบอัปโหลด"""
        self.config = config
        
        # การตั้งค่าพื้นฐาน
        self.watch_folders = self.config.get('watch_folders', [])
        self.telegram_group = self.config.get('telegram_group')
        self.tdl_path = self.config.get('tdl_path')
        self.log_file = self.config.get('log_file', 'media_upload.log')
        self.history_file = self.config.get('history_file', 'upload_history.json')
        
        # การตั้งค่าล็อก
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

        # ชนิดไฟล์สื่อที่รองรับ
        self.media_extensions = self.config.get('media_extensions', [
            '.mp4', '.mkv', '.avi', '.mov', '.webm',  # วิดีโอ
            '.jpg', '.jpeg', '.png', '.gif'           # รูปภาพ
        ])

        # โหลดประวัติการอัปโหลด
        self.upload_history = self._load_history()

        # ตรวจสอบความถูกต้องของเส้นทาง
        self._validate_paths()

    def _validate_paths(self):
        """ตรวจสอบความถูกต้องของเส้นทางที่จำเป็น"""
        if not os.path.exists(self.tdl_path):
            self.logger.error(f"ไม่พบไฟล์ tdl: {self.tdl_path}")
            sys.exit(1)

        for folder in self.watch_folders:
            if not os.path.exists(folder):
                self.logger.warning(f"โฟลเดอร์ไม่มีอยู่: {folder}")

    def _calculate_file_hash(self, filepath: str) -> Optional[str]:
        """คำนวณแฮชของไฟล์ด้วย SHA-256"""
        try:
            hasher = hashlib.sha256()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            self.logger.error(f"ไม่สามารถคำนวณแฮชของ {filepath}: {e}")
            return None

    def _load_history(self) -> Dict:
        """โหลดประวัติการอัปโหลดจากไฟล์"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"โหลดประวัติล้มเหลว: {e}")
            return {}

    def _save_history(self):
        """บันทึกประวัติการอัปโหลด"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.upload_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"บันทึกประวัติล้มเหลว: {e}")

    def _cleanup_old_history(self, days: int = 30):
        """ลบประวัติการอัปโหลดที่เก่าเกินไป"""
        cutoff_date = datetime.now() - timedelta(days=days)
        self.upload_history = {
            k: v for k, v in self.upload_history.items()
            if datetime.fromisoformat(v.get('uploaded_at', datetime.now().isoformat())) > cutoff_date
        }
        self._save_history()

    def _get_file_hash_cached(self, filepath: str) -> Optional[str]:
        """คืนค่าแฮชของไฟล์ โดยคำนวณใหม่เมื่อขนาดหรือเวลาปรับปรุงเปลี่ยน"""
        try:
            stat = os.stat(filepath)
            size = stat.st_size
            mtime = stat.st_mtime

            history_entry = self.upload_history.get(filepath)
            if (
                history_entry
                and history_entry.get('file_size') == size
                and history_entry.get('last_modified') == mtime
            ):
                return history_entry.get('file_hash')

            file_hash = self._calculate_file_hash(filepath)
            if file_hash:
                self.upload_history[filepath] = {
                    **(history_entry or {}),
                    'file_hash': file_hash,
                    'file_size': size,
                    'last_modified': mtime,
                }
                self._save_history()
            return file_hash
        except Exception as e:
            self.logger.error(f"ไม่สามารถอ่านข้อมูลไฟล์ {filepath}: {e}")
            return None

    def _find_unuploaded_files(self) -> List[str]:
        """ค้นหาไฟล์ที่ยังไม่เคยอัปโหลด"""
        unuploaded_files = []
        uploaded_hashes = {
            v.get('file_hash')
            for v in self.upload_history.values()
            if v.get('uploaded_at')
        }

        for folder in self.watch_folders:
            if not os.path.exists(folder):
                continue

            for root, _, files in os.walk(folder):
                for file in files:
                    filepath = os.path.join(root, file)
                    _, ext = os.path.splitext(filepath)

                    if ext.lower() in self.media_extensions:
                        file_hash = self._get_file_hash_cached(filepath)
                        if file_hash and file_hash not in uploaded_hashes:
                            unuploaded_files.append(filepath)

        return unuploaded_files

    def _upload_media(self, media_path: str) -> bool:
        """อัปโหลดไฟล์สื่อไปยัง Telegram"""
        try:
            file_hash = self._get_file_hash_cached(media_path)
            if not file_hash:
                return False

            _, ext = os.path.splitext(media_path)
            upload_params = [
                self.tdl_path, 'up',
                '-p', media_path,
                '-c', self.telegram_group,
                '--delay', '0s',
                '-l', '3'
            ]

            if ext.lower() in ['.jpg', '.jpeg', '.png', '.gif']:
                upload_params.append('--photo')

            self.logger.info(f"เริ่มอัปโหลด: {media_path}")
            
            # เพิ่มการลองใหม่กรณีอัปโหลดล้มเหลว
            max_retries = 3
            for attempt in range(max_retries):
                result = subprocess.run(upload_params, capture_output=True, text=True, encoding='utf-8')
                
                if result.returncode == 0:
                    stat = os.stat(media_path)
                    upload_entry = {
                        'filename': os.path.basename(media_path),
                        'file_hash': file_hash,
                        'uploaded_at': datetime.now().isoformat(),
                        'size_mb': round(stat.st_size / (1024 * 1024), 2),
                        'file_size': stat.st_size,
                        'last_modified': stat.st_mtime,
                        'type': 'photo' if ext.lower() in ['.jpg', '.jpeg', '.png', '.gif'] else 'video'
                    }
                    self.upload_history[media_path] = upload_entry
                    self._save_history()
                    
                    # ลบไฟล์หลังอัปโหลดสำเร็จ
                    try:
                        os.remove(media_path)
                        self.logger.info(f"ลบไฟล์สำเร็จ: {media_path}")
                    except Exception as e:
                        self.logger.error(f"ไม่สามารถลบไฟล์ {media_path}: {e}")
                    
                    return True
                else:
                    self.logger.warning(f"อัปโหลดล้มเหลว รอบที่ {attempt + 1}: {media_path}")
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            self.logger.error(f"อัปโหลดล้มเหลวหลังจากลอง {max_retries} ครั้ง: {media_path}")
            return False

        except Exception as e:
            self.logger.error(f"เกิดข้อผิดพลาดในการอัปโหลด {media_path}: {e}")
            return False

    def process_files(self):
        """ประมวลผลไฟล์ด้วย ThreadPoolExecutor"""
        unuploaded_files = self._find_unuploaded_files()
        
        if not unuploaded_files:
            self.logger.info("ไม่พบไฟล์ใหม่")
            return

        self.logger.info(f"พบไฟล์ใหม่ {len(unuploaded_files)} ไฟล์")
        
        # ใช้ ThreadPoolExecutor เพื่อประมวลผลแบบขนาน
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(self._upload_media, filepath): filepath for filepath in unuploaded_files}
            
            for future in as_completed(futures):
                filepath = futures[future]
                try:
                    result = future.result()
                    if result:
                        self.logger.info(f"อัปโหลดสำเร็จ: {filepath}")
                    else:
                        self.logger.error(f"อัปโหลดล้มเหลว: {filepath}")
                except Exception as e:
                    self.logger.error(f"ข้อผิดพลาดในการประมวลผล {filepath}: {e}")

def main():
    # โหลดการตั้งค่า
    config = MediaUploaderConfig('config.yaml')
    uploader = MediaUploader(config)
    
    # ล้างประวัติเก่า
    uploader._cleanup_old_history()

    uploader.logger.info("เริ่มตรวจสอบโฟลเดอร์...")
    
    try:
        while True:
            uploader.process_files()
            time.sleep(10)
    except KeyboardInterrupt:
        uploader.logger.info("หยุดการทำงานโดยผู้ใช้")
    except Exception as e:
        uploader.logger.error(f"เกิดข้อผิดพลาดในลูปหลัก: {e}")

if __name__ == "__main__":
    main()
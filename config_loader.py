import logging
import yaml


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

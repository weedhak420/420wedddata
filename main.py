import time

from config_loader import MediaUploaderConfig
from uploader import MediaUploader


def main():
    config = MediaUploaderConfig('config.yaml')
    uploader = MediaUploader(config)

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

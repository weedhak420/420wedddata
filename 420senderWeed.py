import os
import sys
import json
import time
import logging
import hashlib
import subprocess
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import gc

# Make yaml import optional
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logging.warning("PyYAML is not installed. Will use JSON for config if needed.")

class MediaUploaderConfig:
    def __init__(self, config_path: str = 'config420.yaml'):
        """Load settings from YAML or JSON file."""
        self.config = {}
        try:
            if not os.path.exists(config_path):
                 raise FileNotFoundError(f"Configuration file not found: {config_path}")

            _, ext = os.path.splitext(config_path)
            if ext.lower() in ('.yaml', '.yml'):
                if not YAML_AVAILABLE:
                    raise ImportError("PyYAML is required for YAML configuration files.")
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f) or {}
            elif ext.lower() == '.json':
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {ext}")

        except (FileNotFoundError, ImportError, ValueError) as e:
            logging.error(e)
            # In case of error, we stop, as a valid config is essential.
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error loading configuration: {e}")
            sys.exit(1)

    def get(self, key: str, default=None):
        """Get configuration value."""
        return self.config.get(key, default)

class MediaUploader:
    def __init__(self, config: MediaUploaderConfig):
        """Initialize the upload system."""
        self.config = config

        # Load settings
        self.watch_folders = self.config.get('watch_folders', [])
        self.telegram_group = self.config.get('telegram_group')
        
        # NEW: Load group mapping for different media types
        self.group_mapping = self.config.get('group_mapping', {})
        
        self.tdl_path = self.config.get('tdl_path')
        self.log_file = self.config.get('log_file', 'media_upload.log')
        self.history_file = self.config.get('history_file', 'upload_history.json')
        self.scan_interval = self.config.get('scan_interval', 60)
        self.max_workers = self.config.get('max_workers', 1)
        self.media_extensions = set(self.config.get('media_extensions', []))

        # Setup logger
        log_level = getattr(logging, self.config.get('log_level', 'INFO').upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Validate paths before proceeding
        self._validate_paths()

        # Load upload history
        self.upload_history = self._load_history()

    def _validate_paths(self):
        """Validate required paths."""
        if not self.tdl_path or not isinstance(self.tdl_path, str):
            self.logger.critical("'tdl_path' is not defined in the config file. Please set the correct path to the tdl executable.")
            sys.exit(1)
        if not os.path.exists(self.tdl_path):
            self.logger.critical(f"tdl executable not found at: {self.tdl_path}")
            sys.exit(1)

        valid_folders = []
        for folder in self.watch_folders:
            if os.path.exists(folder):
                valid_folders.append(folder)
            else:
                self.logger.warning(f"Watch folder does not exist and will be ignored: {folder}")

        if not valid_folders:
            self.logger.critical("No valid 'watch_folders' found. Please check your config.")
            sys.exit(1)
        self.watch_folders = valid_folders

    def _calculate_file_hash(self, filepath: str) -> Optional[str]:
        """Calculate file hash using SHA-256."""
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            self.logger.warning(f"File not found during hash calculation (might have been moved/deleted): {filepath}")
            return None
        except Exception as e:
            self.logger.error(f"Could not calculate hash for {filepath}: {e}")
            return None

    def _load_history(self) -> Dict:
        """Load upload history from file."""
        if not os.path.exists(self.history_file):
            return {}
        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.error(f"History file is corrupted or unreadable. Starting with empty history. Error: {e}")
            return {}

    def _save_history(self):
        """Save upload history."""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.upload_history, f, indent=2, ensure_ascii=False)
        except IOError as e:
            self.logger.error(f"Failed to save history: {e}")

    def _find_unuploaded_files(self) -> List[str]:
        """Find files that have not been uploaded yet."""
        unuploaded_files = []
        for folder in self.watch_folders:
            for root, _, files in os.walk(folder):
                for file in files:
                    filepath = os.path.join(root, file)
                    _, ext = os.path.splitext(filepath)

                    if ext.lower() in self.media_extensions:
                        file_hash = self._calculate_file_hash(filepath)
                        if file_hash and file_hash not in self.upload_history:
                            unuploaded_files.append(filepath)
        return unuploaded_files

    def _get_target_group(self, media_path: str) -> str:
        """
        NEW: Determine which Telegram group to upload to based on file extension.
        Falls back to default telegram_group if no mapping exists.
        """
        _, ext = os.path.splitext(media_path)
        ext_lower = ext.lower()
        
        # Check if group_mapping exists and has configuration
        if self.group_mapping:
            # Check video extensions
            video_exts = self.group_mapping.get('video_extensions', [])
            video_group = self.group_mapping.get('video_group')
            if ext_lower in video_exts and video_group:
                self.logger.debug(f"File {os.path.basename(media_path)} matched video group")
                return video_group
            
            # Check image extensions
            image_exts = self.group_mapping.get('image_extensions', [])
            image_group = self.group_mapping.get('image_group')
            if ext_lower in image_exts and image_group:
                self.logger.debug(f"File {os.path.basename(media_path)} matched image group")
                return image_group
        
        # Fall back to default group
        self.logger.debug(f"File {os.path.basename(media_path)} using default group")
        return self.telegram_group

    def _upload_media(self, media_path: str) -> bool:
        """Upload a single media file to Telegram."""
        file_hash = self._calculate_file_hash(media_path)
        if not file_hash:
            return False

        _, ext = os.path.splitext(media_path)
        
        # NEW: Get target group based on file type
        target_group = self._get_target_group(media_path)
        
        command = [self.tdl_path, 'up', '-p', media_path, '-c', target_group]
        if ext.lower() in {'.jpg', '.jpeg', '.png', '.gif'}:
            command.append('--photo')

        self.logger.info(f"Starting upload for: {os.path.basename(media_path)} to group: {target_group}")

        try:
            result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', check=False)

            if result.returncode == 0:
                self.logger.info(f"Successfully uploaded: {os.path.basename(media_path)} to {target_group}")
                self.upload_history[file_hash] = {
                    'filename': os.path.basename(media_path),
                    'uploaded_at': datetime.now().isoformat(),
                    'target_group': target_group,  # NEW: Track which group was used
                }
                self._save_history()

                # Optionally remove the file after upload
                # To enable, add 'remove_on_success: true' to your config
                if self.config.get('remove_on_success', False):
                    try:
                        os.remove(media_path)
                        self.logger.info(f"Removed local file: {media_path}")
                    except OSError as e:
                        self.logger.error(f"Failed to remove file {media_path}: {e}")

                return True
            else:
                self.logger.error(f"Failed to upload {os.path.basename(media_path)}. "
                                  f"Stderr: {result.stderr.strip()}")
                return False
        except Exception as e:
            self.logger.error(f"An exception occurred during upload of {media_path}: {e}")
            return False

    def process_files(self):
        """Find and upload all new files."""
        self.logger.info("Scanning for new files...")
        unuploaded_files = self._find_unuploaded_files()

        if not unuploaded_files:
            self.logger.info("No new files to upload.")
            return

        self.logger.info(f"Found {len(unuploaded_files)} new file(s). Starting upload process...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._upload_media, filepath): filepath for filepath in unuploaded_files}

            for future in as_completed(futures):
                filepath = futures[future]
                try:
                    success = future.result()
                    if not success:
                        self.logger.warning(f"Upload task for {filepath} failed.")
                except Exception as e:
                    self.logger.error(f"Error processing future for {filepath}: {e}")

    def run(self):
        """Run the media uploader using a polling loop."""
        self.logger.info("Starting Media Uploader in polling mode...")
        self.logger.info(f"Scanning folders every {self.scan_interval} seconds.")

        try:
            while True:
                self.process_files()
                self.logger.info(f"Waiting for {self.scan_interval} seconds before next scan...")
                time.sleep(self.scan_interval)
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        except Exception as e:
            self.logger.critical(f"A critical error occurred in the main loop: {e}")
        finally:
            self.logger.info("Shutdown complete.")

def main():
    """Main entry point."""
    config_path = 'config.yaml'
    if len(sys.argv) > 1:
        config_path = sys.argv[1]

    config = MediaUploaderConfig(config_path)
    uploader = MediaUploader(config)
    uploader.run()

if __name__ == '__main__':
        main()

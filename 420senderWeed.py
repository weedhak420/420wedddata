import os
import sys
import json
import time
import logging
import hashlib
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional
from logging.handlers import RotatingFileHandler
import threading
import sqlite3

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:  # pragma: no cover - watchdog may not be installed in some envs
    Observer = None
    FileSystemEventHandler = object

# Make yaml import optional
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logging.warning("PyYAML is not installed. Will use JSON for config if needed.")

class MediaUploaderConfig:
    def __init__(self, config_path: str = 'config.yaml'):
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
    class _MediaHandler(FileSystemEventHandler):
        """Handle file system events."""

        def __init__(self, uploader: "MediaUploader"):
            self.uploader = uploader

        def on_created(self, event):  # pragma: no cover - integration
            if not getattr(event, "is_directory", False):
                self.uploader._enqueue_file(event.src_path)

        def on_modified(self, event):  # pragma: no cover - integration
            if not getattr(event, "is_directory", False):
                self.uploader._enqueue_file(event.src_path)

    def __init__(self, config: MediaUploaderConfig):
        """Initialize the upload system."""
        self.config = config

        # Load settings
        self.watch_folders = self.config.get('watch_folders', [])
        self.telegram_group = self.config.get('telegram_group')
        self.tdl_path = self.config.get('tdl_path')
        self.log_file = self.config.get('log_file', 'media_upload.log')
        self.history_db = self.config.get('history_db', 'upload_history.db')
        self.max_workers = self.config.get('max_workers', os.cpu_count() or 1)
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay = self.config.get('retry_delay', 5)
        self.media_extensions = set(self.config.get('media_extensions', []))
        self.log_max_bytes = self.config.get('log_max_bytes', 10 * 1024 * 1024)
        self.log_backup_count = self.config.get('log_backup_count', 5)

        # Setup logger with rotation
        log_level = getattr(logging, self.config.get('log_level', 'INFO').upper(), logging.INFO)
        rotating_handler = RotatingFileHandler(
            self.log_file, maxBytes=self.log_max_bytes, backupCount=self.log_backup_count, encoding='utf-8'
        )
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[rotating_handler, logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

        # Validate paths before proceeding
        self._validate_paths()

        if Observer is None:
            self.logger.critical("watchdog library is required for file watching.")
            sys.exit(1)

        # Initialize executor and state holders
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.pending_lock = threading.Lock()
        self.pending: set[str] = set()

        # Initialize history database
        self.db_lock = threading.Lock()
        self._init_db()

        # Setup observer for file system events
        self.observer = Observer()
        self.event_handler = self._MediaHandler(self)

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
        
    def _init_db(self) -> None:
        """Initialise SQLite database for upload history."""
        with self.db_lock:
            self.conn = sqlite3.connect(self.history_db, check_same_thread=False)
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploads (
                    path TEXT PRIMARY KEY,
                    hash TEXT,
                    size INTEGER,
                    mtime REAL,
                    uploaded_at TEXT
                )
                """
            )
            self.conn.commit()

    def _is_media_file(self, path: str) -> bool:
        _, ext = os.path.splitext(path)
        return ext.lower() in self.media_extensions

    def _needs_upload(self, path: str) -> bool:
        if not os.path.isfile(path):
            return False
        stat = os.stat(path)
        with self.db_lock:
            cur = self.conn.execute("SELECT size, mtime FROM uploads WHERE path=?", (path,))
            row = cur.fetchone()
        return not (row and row[0] == stat.st_size and row[1] == stat.st_mtime)

    def _calculate_file_hash(self, filepath: str, block_size: int = 1024 * 1024) -> Optional[str]:
        """Calculate file hash using SHA-256."""
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(block_size), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            self.logger.warning(f"File not found during hash calculation (might have been moved/deleted): {filepath}")
            return None
        except Exception as e:
            self.logger.error(f"Could not calculate hash for {filepath}: {e}")
            return None

    def _enqueue_file(self, path: str) -> None:
        if not self._is_media_file(path):
            return
        with self.pending_lock:
            if path in self.pending:
                return
            self.pending.add(path)
        self.executor.submit(self._process_file, path)

    def _process_file(self, path: str) -> None:
        try:
            if self._needs_upload(path):
                success = self._upload_media(path)
                if not success:
                    self.logger.warning(f"Upload task for {path} failed.")
        finally:
            with self.pending_lock:
                self.pending.discard(path)

    def _upload_media(self, media_path: str) -> bool:
        """Upload a single media file to Telegram with retry logic."""
        file_hash = self._calculate_file_hash(media_path)
        if not file_hash:
            return False

        _, ext = os.path.splitext(media_path)
        command = [self.tdl_path, 'up', '-p', media_path, '-c', self.telegram_group]
        if ext.lower() in {'.jpg', '.jpeg', '.png', '.gif'}:
            command.append('--photo')

        for attempt in range(1, self.max_retries + 1):
            self.logger.info(f"Starting upload for: {os.path.basename(media_path)} (attempt {attempt})")
            try:
                result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', check=False)

                if result.returncode == 0:
                    self.logger.info(f"Successfully uploaded: {os.path.basename(media_path)}")
                    stat = os.stat(media_path)
                    with self.db_lock:
                        self.conn.execute(
                            "INSERT OR REPLACE INTO uploads (path, hash, size, mtime, uploaded_at) VALUES (?,?,?,?,?)",
                            (media_path, file_hash, stat.st_size, stat.st_mtime, datetime.now().isoformat()),
                        )
                        self.conn.commit()

                    if self.config.get('remove_on_success', False):
                        try:
                            os.remove(media_path)
                            self.logger.info(f"Removed local file: {media_path}")
                        except OSError as e:
                            self.logger.error(f"Failed to remove file {media_path}: {e}")

                    return True
                else:
                    self.logger.error(
                        f"Failed to upload {os.path.basename(media_path)}. Stderr: {result.stderr.strip()}"
                    )
            except Exception as e:
                self.logger.error(f"An exception occurred during upload of {media_path}: {e}")

            if attempt < self.max_retries:
                backoff = self.retry_delay * attempt
                self.logger.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)

        return False

    def _iter_files(self, folder: str):
        try:
            with os.scandir(folder) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        yield from self._iter_files(entry.path)
                    elif entry.is_file(follow_symlinks=False) and self._is_media_file(entry.path):
                        yield entry.path
        except Exception as e:
            self.logger.error(f"Error scanning folder {folder}: {e}")

    def _find_unuploaded_files(self) -> List[str]:
        unuploaded_files = []
        for folder in self.watch_folders:
            for path in self._iter_files(folder):
                if self._needs_upload(path):
                    unuploaded_files.append(path)
        return unuploaded_files

    def process_files(self):
        """Find and upload all new files in the watch folders."""
        self.logger.info("Scanning for new files...")
        unuploaded_files = self._find_unuploaded_files()

        if not unuploaded_files:
            self.logger.info("No new files to upload.")
            return

        self.logger.info(f"Found {len(unuploaded_files)} new file(s). Starting upload process...")

        futures = [self.executor.submit(self._upload_media, filepath) for filepath in unuploaded_files]
        for future in as_completed(futures):
            try:
                if not future.result():
                    self.logger.warning("An upload task failed.")
            except Exception as e:
                self.logger.error(f"Error processing future: {e}")

    def run(self):
        """Run the media uploader using file system events."""
        self.logger.info("Starting Media Uploader with watchdog...")
        for folder in self.watch_folders:
            self.observer.schedule(self.event_handler, folder, recursive=True)

        # Initial scan to catch existing files
        self.process_files()

        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        except Exception as e:
            self.logger.critical(f"A critical error occurred in the main loop: {e}")
        finally:
            self.observer.stop()
            self.observer.join()
            self.executor.shutdown(wait=True)
            with self.db_lock:
                self.conn.close()
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
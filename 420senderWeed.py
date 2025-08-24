import os
import sys
import json
import time
import logging
import hashlib
import subprocess
import signal
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional
from logging.handlers import RotatingFileHandler
import threading
import sqlite3

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init()
    COLOR_AVAILABLE = True
except Exception:  # pragma: no cover - colorama optional
    COLOR_AVAILABLE = False

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


class UploadMetrics:
    """Track upload statistics for health reporting."""

    def __init__(self) -> None:
        self.start_time = time.time()
        self.total = 0
        self.success = 0
        self.failed = 0
        self.bytes_uploaded = 0
        self.lock = threading.Lock()

    def record_success(self, size: int) -> None:
        with self.lock:
            self.total += 1
            self.success += 1
            self.bytes_uploaded += size

    def record_failure(self) -> None:
        with self.lock:
            self.total += 1
            self.failed += 1

    def snapshot(self) -> dict:
        with self.lock:
            total = self.total
            success = self.success
            failed = self.failed
            bytes_uploaded = self.bytes_uploaded
        rate = success / total if total else 0
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "bytes": bytes_uploaded,
            "success_rate": rate,
            "elapsed": time.time() - self.start_time,
        }


class EmojiFormatter(logging.Formatter):
    EMOJI = {
        logging.DEBUG: "🐛",
        logging.INFO: "ℹ️",
        logging.WARNING: "⚠️",
        logging.ERROR: "❌",
        logging.CRITICAL: "💥",
    }
    COLORS = {
        logging.DEBUG: getattr(Fore, "BLUE", ""),
        logging.INFO: getattr(Fore, "GREEN", ""),
        logging.WARNING: getattr(Fore, "YELLOW", ""),
        logging.ERROR: getattr(Fore, "RED", ""),
        logging.CRITICAL: getattr(Fore, "MAGENTA", ""),
    }

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting
        emoji = self.EMOJI.get(record.levelno, "")
        if COLOR_AVAILABLE:
            color = self.COLORS.get(record.levelno, "")
            record.msg = f"{color}{record.msg}{Style.RESET_ALL}"
        record.msg = f"{emoji} {record.msg}"
        return super().format(record)

class MediaUploader:
    class _MediaHandler(FileSystemEventHandler):
        """Handle file system events."""

        def __init__(self, uploader: "MediaUploader"):
            self.uploader = uploader

        def on_created(self, event):  # pragma: no cover - integration
            if not getattr(event, "is_directory", False):
                self.uploader._enqueue_file(Path(event.src_path))

        def on_modified(self, event):  # pragma: no cover - integration
            if not getattr(event, "is_directory", False):
                self.uploader._enqueue_file(Path(event.src_path))

    def __init__(self, config: MediaUploaderConfig):
        """Initialize the upload system."""
        self.config = config

        # Load settings
        cpu_count = os.cpu_count() or 1
        self.watch_folders = [Path(p) for p in self.config.get('watch_folders', [])]
        self.telegram_group = self.config.get('telegram_group')
        self.tdl_path = Path(self.config.get('tdl_path'))
        self.log_file = self.config.get('log_file', 'media_upload.log')
        self.history_db = self.config.get('history_db', 'upload_history.db')
        self.max_workers = min(self.config.get('max_workers', cpu_count), cpu_count)
        self.max_concurrent_uploads = self.config.get('max_concurrent_uploads', self.max_workers)
        self.cooldown_period = self.config.get('cooldown_period', 0)
        self.upload_timeout = self.config.get('upload_timeout', 300)
        self.batch_size = self.config.get('batch_size', 10)
        self.max_file_size = self.config.get('max_file_size', 50 * 1024 * 1024)
        self.health_report_interval = self.config.get('health_report_interval', 300)
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
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = EmojiFormatter('%(asctime)s - %(levelname)s: %(message)s')
        rotating_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logging.basicConfig(level=log_level, handlers=[rotating_handler, stream_handler])
        self.logger = logging.getLogger(__name__)

        # Validate paths before proceeding
        self._validate_paths()

        if Observer is None:
            self.logger.critical("watchdog library is required for file watching.")
            sys.exit(1)

        # Initialize executor and state holders
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.upload_semaphore = threading.Semaphore(self.max_concurrent_uploads)
        self.pending_lock = threading.Lock()
        self.pending: set[Path] = set()

        # Metrics and health monitor
        self.metrics = UploadMetrics()
        self.stop_event = threading.Event()
        self.health_thread = threading.Thread(target=self._health_monitor, daemon=True)

        # Initialize history database
        self.db_lock = threading.Lock()
        self._init_db()

        # Setup observer for file system events
        self.observer = Observer()
        self.event_handler = self._MediaHandler(self)

        # Handle graceful shutdown signals
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _validate_paths(self):
        """Validate required paths."""
        if not self.tdl_path or not isinstance(self.tdl_path, Path):
            self.logger.critical("'tdl_path' is not defined in the config file. Please set the correct path to the tdl executable.")
            sys.exit(1)
        if not self.tdl_path.exists():
            self.logger.critical(f"tdl executable not found at: {self.tdl_path}")
            sys.exit(1)

        valid_folders: List[Path] = []
        for folder in self.watch_folders:
            if folder.exists():
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
                    uploaded_at TEXT,
                    upload_duration REAL,
                    retry_count INTEGER
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS failed_uploads (
                    path TEXT PRIMARY KEY,
                    error TEXT,
                    last_attempt TEXT,
                    retry_count INTEGER
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_uploads_hash ON uploads(hash)"
            )
            self.conn.commit()

    def _is_media_file(self, path: Path) -> bool:
        return path.suffix.lower() in self.media_extensions

    def _needs_upload(self, path: Path) -> bool:
        if not path.is_file():
            return False
        stat = path.stat()
        if stat.st_size > self.max_file_size:
            self.logger.info(f"Skipping {path} due to size limit")
            return False
        with self.db_lock:
            cur = self.conn.execute(
                "SELECT size, mtime FROM uploads WHERE path=?", (str(path),)
            )
            row = cur.fetchone()
            cur_fail = self.conn.execute(
                "SELECT retry_count FROM failed_uploads WHERE path=?", (str(path),)
            )
            fail_row = cur_fail.fetchone()
        if fail_row and fail_row[0] >= self.max_retries:
            return False
        return not (row and row[0] == stat.st_size and row[1] == stat.st_mtime)

    def _calculate_file_hash(self, filepath: Path, block_size: int = 1024 * 1024) -> Optional[str]:
        """Calculate file hash using SHA-256."""
        hasher = hashlib.sha256()
        try:
            with filepath.open('rb') as f:
                for chunk in iter(lambda: f.read(block_size), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except FileNotFoundError:
            self.logger.warning(f"File not found during hash calculation (might have been moved/deleted): {filepath}")
            return None
        except Exception as e:
            self.logger.error(f"Could not calculate hash for {filepath}: {e}")
            return None

    def _enqueue_file(self, path: Path) -> None:
        if not self._is_media_file(path):
            return
        with self.pending_lock:
            if path in self.pending:
                return
            self.pending.add(path)
        self.executor.submit(self._process_file, path)

    def _process_file(self, path: Path) -> None:
        try:
            if self._needs_upload(path):
                with self.upload_semaphore:
                    success = self._upload_media(path)
                    if not success:
                        self.logger.warning(f"Upload task for {path} failed.")
                    if self.cooldown_period:
                        time.sleep(self.cooldown_period)
        finally:
            with self.pending_lock:
                self.pending.discard(path)

    def _upload_media(self, media_path: Path) -> bool:
        """Upload a single media file to Telegram with retry logic."""
        file_hash = self._calculate_file_hash(media_path)
        if not file_hash:
            return False

        command = [str(self.tdl_path), 'up', '-p', str(media_path), '-c', self.telegram_group]
        if media_path.suffix.lower() in {'.jpg', '.jpeg', '.png', '.gif'}:
            command.append('--photo')

        start_time = time.time()
        last_error = ''
        for attempt in range(1, self.max_retries + 1):
            self.logger.info(f"Starting upload for: {media_path.name} (attempt {attempt})")
            try:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    timeout=self.upload_timeout,
                    check=False,
                )
                stdout = result.stdout.strip()
                stderr = result.stderr.strip()
                if stdout:
                    self.logger.debug(stdout)
                if result.returncode == 0:
                    duration = time.time() - start_time
                    stat = media_path.stat()
                    self.logger.info(f"Successfully uploaded: {media_path.name}")
                    with self.db_lock:
                        self.conn.execute(
                            "INSERT OR REPLACE INTO uploads (path, hash, size, mtime, uploaded_at, upload_duration, retry_count) VALUES (?,?,?,?,?,?,?)",
                            (str(media_path), file_hash, stat.st_size, stat.st_mtime, datetime.now().isoformat(), duration, attempt),
                        )
                        self.conn.execute("DELETE FROM failed_uploads WHERE path=?", (str(media_path),))
                        self.conn.commit()
                    self.metrics.record_success(stat.st_size)
                    if self.config.get('remove_on_success', False):
                        try:
                            media_path.unlink()
                            self.logger.info(f"Removed local file: {media_path}")
                        except OSError as e:
                            self.logger.error(f"Failed to remove file {media_path}: {e}")
                    return True
                else:
                    last_error = stderr or stdout
                    self.logger.error(f"Failed to upload {media_path.name}. stderr: {stderr or 'N/A'}")
                    if 'Too Many Requests' in stderr:
                        match = re.search(r'retry after (\d+)', stderr)
                        if match:
                            wait = int(match.group(1))
                            self.logger.warning(f"Rate limited. Waiting {wait}s...")
                            time.sleep(wait)
                            continue
            except subprocess.TimeoutExpired:
                last_error = 'timeout'
                self.logger.error(f"Upload timed out for {media_path.name}")
            except Exception as e:
                last_error = str(e)
                self.logger.error(f"An exception occurred during upload of {media_path}: {e}")

            if attempt < self.max_retries:
                backoff = self.retry_delay * (2 ** (attempt - 1))
                self.logger.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)

        duration = time.time() - start_time
        with self.db_lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO failed_uploads (path, error, last_attempt, retry_count) VALUES (?,?,?,?)",
                (str(media_path), last_error, datetime.now().isoformat(), self.max_retries),
            )
            self.conn.commit()
        self.metrics.record_failure()
        return False

    def _health_monitor(self) -> None:  # pragma: no cover - background thread
        while not self.stop_event.wait(self.health_report_interval):
            snap = self.metrics.snapshot()
            self.logger.info(
                f"Health check: total={snap['total']} success={snap['success']} failed={snap['failed']} bytes={snap['bytes']}"
            )

    def _handle_shutdown(self, signum, frame):  # pragma: no cover - signal handling
        self.logger.info(f"Received signal {signum}. Initiating shutdown...")
        self.stop_event.set()

    def _iter_files(self, folder: Path):
        try:
            with os.scandir(folder) as it:
                for entry in it:
                    p = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        yield from self._iter_files(p)
                    elif entry.is_file(follow_symlinks=False) and self._is_media_file(p):
                        yield p
        except Exception as e:
            self.logger.error(f"Error scanning folder {folder}: {e}")

    def _find_unuploaded_files(self) -> List[Path]:
        unuploaded_files: List[Path] = []
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

        for i in range(0, len(unuploaded_files), self.batch_size):
            batch = unuploaded_files[i:i + self.batch_size]
            futures = [self.executor.submit(self._process_file, p) for p in batch]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing future: {e}")

    def run(self):
        """Run the media uploader using file system events."""
        self.logger.info("Starting Media Uploader with watchdog...")
        for folder in self.watch_folders:
            self.observer.schedule(self.event_handler, folder, recursive=True)

        # Initial scan to catch existing files
        self.process_files()

        self.health_thread.start()
        self.observer.start()
        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt. Shutting down...")
            self.stop_event.set()
        except Exception as e:
            self.logger.critical(f"A critical error occurred in the main loop: {e}")
            self.stop_event.set()
        finally:
            self.observer.stop()
            self.observer.join()
            self.executor.shutdown(wait=True)
            self.health_thread.join(timeout=5)
            with self.db_lock:
                self.conn.close()
            snap = self.metrics.snapshot()
            self.logger.info(
                f"Final stats: total={snap['total']} success={snap['success']} failed={snap['failed']} bytes={snap['bytes']}"
            )
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

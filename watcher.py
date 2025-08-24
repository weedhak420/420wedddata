from watchdog.events import FileSystemEventHandler
import os


class Watcher(FileSystemEventHandler):
    """Handles filesystem events for new media files."""

    def __init__(self, uploader):
        self.uploader = uploader

    def on_created(self, event):
        if event.is_directory:
            return
        _, ext = os.path.splitext(event.src_path)
        if ext.lower() in self.uploader.media_extensions:
            self.uploader._upload_media(event.src_path)


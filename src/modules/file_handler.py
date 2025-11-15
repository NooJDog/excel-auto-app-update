# modules/file_handler.py
import os
import shutil
import datetime

class FileHandler:
    def __init__(self, config):
        self.config = config
        # archive folder relative to project root
        base = os.path.dirname(os.path.dirname(__file__))
        self.archive_folder = os.path.join(base, self.config.get("archive_folder", "processed"))
        os.makedirs(self.archive_folder, exist_ok=True)

    def archive_file(self, filepath, move=False):
        """
        Archive the processed file.
        If move=True, try to move; otherwise copy and keep original.
        Returns destination path (or None on failure).
        """
        if not os.path.isfile(filepath):
            return None
        basename = os.path.basename(filepath)
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(self.archive_folder, f"{stamp}_{basename}")
        try:
            if move:
                shutil.move(filepath, dest)
            else:
                shutil.copy2(filepath, dest)
            return dest
        except Exception:
            try:
                # fallback: try copy then remove original if move requested
                shutil.copy2(filepath, dest)
                if move:
                    os.remove(filepath)
                return dest
            except Exception:
                return None
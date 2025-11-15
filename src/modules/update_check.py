import requests, hashlib, os, zipfile, tempfile, shutil
from typing import Callable, Dict, Any, Set

DEFAULT_SKIP: Set[str] = {
    "config.json",
    "db/transactions.db",
    "logs/",
    # 跳過可能鎖住的主執行檔（Windows EXE）
    "excel_auto_app.exe"
}

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest().lower()

class UpdateManager:
    def __init__(self, manifest_url: str, app_version: str, app_dir: str,
                 skip_paths: Set[str] = None):
        self.manifest_url = manifest_url
        self.app_version = app_version
        self.app_dir = app_dir
        self.skip_paths = set(skip_paths or []) | DEFAULT_SKIP

    def _is_skipped(self, rel_path: str) -> bool:
        rp = rel_path.replace("\\", "/")
        for s in self.skip_paths:
            s = s.replace("\\", "/")
            if rp == s or rp.startswith(s):
                return True
        return False

    def fetch_manifest(self) -> Dict[str, Any]:
        r = requests.get(self.manifest_url, timeout=10)
        r.raise_for_status()
        return r.json()

    def need_update(self, mf: Dict[str, Any]) -> bool:
        latest = mf.get("latest_version")
        return bool(latest and str(latest) != str(self.app_version))

    def download_full_package(self, url: str, expect_sha256: str,
                              on_progress: Callable[[int, str], None]) -> str:
        """
        下載更新包（zip），回傳本機暫存路徑；on_progress(百分比, 狀態文字)
        """
        on_progress(0, "下載更新包中...")
        tmp_zip = os.path.join(tempfile.gettempdir(), "__update_pkg.zip")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0)) or None
            wrote = 0
            with open(tmp_zip, "wb") as f:
                for chunk in r.iter_content(8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    wrote += len(chunk)
                    if total:
                        pct = int(wrote / total * 100)
                        on_progress(min(pct, 99), f"下載中 {pct}%")
        have = sha256_file(tmp_zip)
        if have != expect_sha256.lower():
            try: os.remove(tmp_zip)
            except: pass
            raise RuntimeError("更新包 SHA256 校驗失敗")
        on_progress(99, "下載完成，校驗通過")
        return tmp_zip

    def extract_and_copy(self, zip_path: str, on_progress: Callable[[int, str], None]) -> int:
        """
        解壓並覆蓋到 app_dir，跳過 skip_paths；回傳覆蓋檔案數。
        """
        on_progress(0, "解壓更新包...")
        tmp_dir = tempfile.mkdtemp(prefix="upd_")
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(tmp_dir)
            # 嘗試抓 ZIP 內第一層資料夾（PyInstaller 通常包一層）
            root_entries = os.listdir(tmp_dir)
            if len(root_entries) == 1 and os.path.isdir(os.path.join(tmp_dir, root_entries[0])):
                src_root = os.path.join(tmp_dir, root_entries[0])
            else:
                src_root = tmp_dir

            # 統計檔案總數用於進度
            files = []
            for r, _, fs in os.walk(src_root):
                for fn in fs:
                    rel = os.path.relpath(os.path.join(r, fn), src_root)
                    if self._is_skipped(rel):
                        continue
                    files.append(rel)

            total = max(len(files), 1)
            copied = 0
            for rel in files:
                src = os.path.join(src_root, rel)
                dst = os.path.join(self.app_dir, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                copied += 1
                pct = int(copied / total * 100)
                on_progress(pct, f"覆蓋檔案 {copied}/{total} ({pct}%)")

            on_progress(100, "更新完成")
            return copied
        finally:
            try: os.remove(zip_path)
            except: pass
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def run_update(self, on_progress: Callable[[int, str], None]) -> Dict[str, Any]:
        """
        主流程：抓 manifest → 判斷版本 → 下載 → 覆蓋
        """
        try:
            on_progress(0, "檢查更新...")
            mf = self.fetch_manifest()
        except Exception as e:
            return {"ok": False, "error": f"讀取 manifest 失敗: {e}"}

        if not self.need_update(mf):
            return {"ok": True, "updated": False, "message": "已是最新版本"}

        pkg = mf.get("full_package") or {}
        url = pkg.get("url")
        sha = (pkg.get("sha256") or "").lower()
        if not url or not sha:
            return {"ok": False, "error": "manifest 缺少 full_package.url 或 sha256"}

        try:
            zip_path = self.download_full_package(url, sha, on_progress)
            count = self.extract_and_copy(zip_path, on_progress)
            return {"ok": True, "updated": True, "count": count, "latest_version": mf.get("latest_version")}
        except Exception as e:
            return {"ok": False, "error": str(e)}
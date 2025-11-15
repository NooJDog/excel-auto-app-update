import os
import sys
from pathlib import Path

def _base_path() -> Path:
    """
    取得專案根目錄 excel_auto_app (不再回傳 src)。
    適用情境：
      開發結構：excel_auto_app/
        ├─ src/
        │   └─ modules/resources.py
        ├─ input/
        ├─ assets/
        ├─ fonts/
        └─ config.json
      打包 (PyInstaller onefile / onefolder)：
        frozen 狀態下使用執行目錄或 _MEIPASS。
    """
    if getattr(sys, "frozen", False):
        # PyInstaller onefile 有 _MEIPASS
        if hasattr(sys, "_MEIPASS"):
            return Path(sys._MEIPASS)
        # one-folder：工作目錄即為根
        return Path(os.getcwd())

    here = Path(__file__).resolve()
    # 尋找 src 目錄，回到其父層 (excel_auto_app)
    for p in here.parents:
        if p.name == "src":
            return p.parent
    # fallback：modules → src → excel_auto_app 的上一層上一層
    return here.parent.parent

def project_root() -> Path:
    """
    回傳 excel_auto_app 專案根目錄。
    """
    return _base_path()

def get_input_path(*parts) -> Path:
    """
    取得 input 目錄下的路徑：
      get_input_path() -> <root>/input
      get_input_path('A.xlsx') -> <root>/input/A.xlsx
    """
    return project_root() / "input" / Path(*parts)

def get_asset_path(*parts) -> Path:
    return project_root() / "assets" / Path(*parts)

def get_font_path(*parts) -> Path:
    return project_root() / "fonts" / Path(*parts)

def get_config_path() -> Path:
    return project_root() / "config.json"

def ensure_exists(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"資源不存在: {path}")
    return path

def locate_template() -> Path:
    # 依需求可改成其他檔名
    return ensure_exists(get_asset_path("templates", "S_36.png"))

def locate_default_font() -> Path:
    for name in ["NotoSansTC-Regular.ttf", "NotoSansTC-Regular.otf", "msjh.ttf", "msjh.ttc"]:
        p = get_font_path(name)
        if p.exists():
            return p
    return None
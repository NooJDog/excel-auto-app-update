import os
from pathlib import Path

def output_root(base: Path) -> Path:
    return base / "輸出資料"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def chat_images_product_dir(base: Path, product_type: str) -> Path:
    product_map = {
        "game_currency": "遊戲幣",
        "game_item": "遊戲寶物",
        "used_goods": "二手商品"
    }
    pname = product_map.get(product_type, "遊戲幣")
    return ensure_dir(base / "聊天圖片" / pname)

def woo_export_dir(base: Path) -> Path:
    return ensure_dir(base / "WOO匯出")

def match_report_dir(base: Path) -> Path:
    return ensure_dir(base / "媒合報表")

def logs_dir(base: Path) -> Path:
    return ensure_dir(base / "日誌紀錄")
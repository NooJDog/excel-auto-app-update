import sys
import os
import traceback
import sqlite3
import random
import time
import json
import platform
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyQt6 import QtWidgets, QtGui, QtCore

from version import __version__
from ui_main_py import Ui_MainWindow
from modules.resources import project_root, get_config_path
from modules.config_manager import ConfigManager
from modules.db_manager import DBManager
from modules.excel_parser import ExcelParser
from modules.bank_excel_converter import process_file as bank_convert
from modules.chat_image_generator import generate_images_from_records
from modules.match_report import generate_match_reports
from modules.theme_styles import ENHANCED_QSS
from output_paths import (
    output_root, chat_images_product_dir, woo_export_dir,
    match_report_dir, logs_dir
)
from modules.woo_client import WooClient
from single_instance import acquire_lock, release_lock

# 更新檢查模組（GitHub Raw manifest）
try:
    from modules.update_check import UpdateManager
    from modules.update_worker import UpdateWorker
except ImportError:
    UpdateManager = None
    UpdateWorker = None

APP_NAME = f"Y.J v{__version__}"
ROOT_DIR = project_root()

PRODUCT_CODE_MAP = {
    "遊戲幣": "game_currency",
    "遊戲寶物": "game_item",
    "二手商品": "used_goods"
}
ALL_PRODUCT_CODES = list(PRODUCT_CODE_MAP.values())
PRODUCT_CN_MAP = {
    "game_currency": "遊戲幣",
    "game_item": "遊戲寶物",
    "used_goods": "二手商品"
}


def is_frozen():
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def runtime_root() -> Path:
    if is_frozen():
        return Path(sys._MEIPASS)
    return project_root()


def asset_path(*parts) -> Path:
    return runtime_root() / "assets" / Path(*parts)


class RunLogger:
    def __init__(self, base_dir: str):
        os.makedirs(base_dir, exist_ok=True)
        self.base_dir = base_dir
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.exec_path = os.path.join(base_dir, f"run_{self.run_id}.log")
        self.err_path = os.path.join(base_dir, f"errors_{self.run_id}.log")
        self._exec_fp = open(self.exec_path, "a", encoding="utf-8")
        self._err_fp = open(self.err_path, "a", encoding="utf-8")
        sys.excepthook = self._global_excepthook

    def _ts(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def info(self, msg: str):
        self._exec_fp.write(f"[{self._ts()}] INFO {msg}\n")
        self._exec_fp.flush()

    def warn(self, msg: str):
        self._exec_fp.write(f"[{self._ts()}] WARN {msg}\n")
        self._exec_fp.flush()

    def error(self, msg: str, exc: Exception = None):
        stack = ""
        if exc:
            stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self._exec_fp.write(f"[{self._ts()}] ERROR {msg}\n")
        self._exec_fp.flush()
        self._err_fp.write(f"[{self._ts()}] ERROR {msg}\n")
        if stack:
            self._err_fp.write(stack + "\n")
        self._err_fp.flush()

    def _global_excepthook(self, etype, value, tb):
        self.error(f"Unhandled exception: {value}", value)

    def close(self):
        self.info("RUN END")
        for fp in (self._exec_fp, self._err_fp):
            try:
                fp.close()
            except:
                pass


def ensure_base_dirs():
    base = output_root(ROOT_DIR)
    for code in ALL_PRODUCT_CODES:
        chat_images_product_dir(base, code)
    woo_export_dir(base)
    match_report_dir(base)
    logs_dir(base)


def clear_all_transactions(db_path):
    if db_path == ":memory:":
        return
    if not os.path.exists(db_path):
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        cur.execute("DELETE FROM transactions")
        conn.commit()
    except Exception as e:
        print("[CLEAR ERROR]", e)
    finally:
        conn.close()


def open_dir(path: str):
    try:
        if os.path.isdir(path):
            QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(path))
    except:
        pass


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, config: ConfigManager):
        super().__init__()
        self.config = config
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.progressBar = self.ui.progressBar
        self.setWindowTitle(APP_NAME)
        self.setStyleSheet(ENHANCED_QSS)

        icon_ico = asset_path("icons", "app.ico")
        icon_png = asset_path("icons", "app.png")
        if icon_ico.exists():
            self.setWindowIcon(QtGui.QIcon(str(icon_ico)))
        elif icon_png.exists():
            self.setWindowIcon(QtGui.QIcon(str(icon_png)))

        self.log_dir_path = logs_dir(output_root(ROOT_DIR))
        self.run_logger = RunLogger(str(self.log_dir_path))
        self.run_logger.info("GUI INIT start")

        use_memory = self.config.get("use_memory_db", False)
        self.db_path = ":memory:" if use_memory else str(ROOT_DIR / self.config.get("db_path", "db/transactions.db"))
        if self.config.get("discard_db_each_start", True):
            clear_all_transactions(self.db_path)
            self.run_logger.info("DB cleared at start")

        self.db = DBManager(self.db_path)
        self.parser = ExcelParser(self.db)
        self.enable_woo = self.config.get("enable_woo_sync", True)

        # 主頁按鈕 / 訊號
        self.ui.btnRunAll.clicked.connect(self.run_selected_tasks)
        self.ui.btnRescanInput.clicked.connect(self.rescan_input_folder)
        self.ui.btnOpenOutput.clicked.connect(self.open_output_folder)
        self.ui.btnRemoveSelected.clicked.connect(self.remove_selected_files)
        self.ui.btnClearList.clicked.connect(self.clear_file_list)
        self.ui.chkAutoClear.setChecked(self.config.get("discard_db_each_start", True))
        self.ui.chkAutoClear.stateChanged.connect(self.on_toggle_auto_clear)
        self.ui.btnClearNow.clicked.connect(self.on_clear_now)
        self.ui.listFiles.itemSelectionChanged.connect(self.refresh_preview)
        self.ui.listFiles.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.ui.listFiles.customContextMenuRequested.connect(self.show_list_context_menu)
        self.ui.listFiles.itemDoubleClicked.connect(self.open_file_location)
        self.ui.cbProductMode.currentIndexChanged.connect(self.on_product_mode_changed)

        # 設定頁
        self.ui.btnSaveSettings.clicked.connect(self.on_save_settings)
        self.ui.btnResetSettings.clicked.connect(self.on_reset_settings)
        self.ui.spinFeeRate.setValue(self.config.get("platform_fee_rate", 0.07) * 100)

        # 檢查更新（主頁工具區）
        if hasattr(self.ui, "btnCheckUpdate") and UpdateManager and UpdateWorker:
            self.ui.btnCheckUpdate.clicked.connect(self.on_check_update)

        # 勾選式第四任務：chkTaskUploadOrders
        # 不再使用 btnWooUploadAndCreate 按鈕；若 UI 仍有該屬性安全忽略
        if hasattr(self.ui, "btnWooUploadAndCreate"):
            # 避免舊 UI 留存造成點擊行為 (不再使用)
            self.ui.btnWooUploadAndCreate.setDisabled(True)
            self.ui.btnWooUploadAndCreate.hide()

        if not self.enable_woo:
            idx = self.ui.tabWidget.indexOf(self.ui.tabWoo)
            if idx != -1:
                self.ui.tabWidget.removeTab(idx)
        else:
            self.init_woo_tab()

        self.setAcceptDrops(True)
        self.on_product_mode_changed()
        self.update_status()
        self.center_window()
        self.run_logger.info("GUI INIT done")

    # --- Woo 設定頁 ---
    def init_woo_tab(self):
        self.ui.btnWooSave.clicked.connect(self.on_woo_save)
        self.ui.btnWooTest.clicked.connect(self.on_woo_test)
        self.ui.editWooUrl.setText(self.config.get("woo_url", ""))
        self.ui.editWooCK.setText(self.config.get("woo_consumer_key", ""))
        self.ui.editWooCS.setText(self.config.get("woo_consumer_secret", ""))
        self.ui.spinWooFeeProduct.setValue(int(self.config.get("woo_fee_product_id", 30977)))
        self.ui.spinWooBatch.setValue(int(self.config.get("woo_batch_size", 200)))
        self.ui.spinWooTimeout.setValue(int(self.config.get("woo_timeout", 15)))
        self.ui.spinWooRemoteScan.setValue(int(self.config.get("woo_remote_dup_scan_limit", 200)))
        self.ui.spinWooWorkers.setValue(int(self.config.get("woo_parallel_workers", 6)))
        self.ui.chkWooTestMode.setChecked(self.config.get("woo_test_mode", True))
        self.ui.chkWooSetCreated.setChecked(self.config.get("woo_set_created_time", True))

    def build_woo_client(self) -> WooClient:
        return WooClient(
            base_url=self.config.get("woo_url", "").strip(),
            consumer_key=self.config.get("woo_consumer_key", "").strip(),
            consumer_secret=self.config.get("woo_consumer_secret", "").strip(),
            timeout=int(self.config.get("woo_timeout", 15)),
            test_mode=bool(self.config.get("woo_test_mode", True)),
            logger=self.run_logger,
            remote_dup_scan_limit=int(self.ui.spinWooRemoteScan.value()),
            set_created_time=self.ui.chkWooSetCreated.isChecked()
        )

    # --- 更新流程 ---
    def on_check_update(self):
        if not UpdateManager or not UpdateWorker:
            QtWidgets.QMessageBox.warning(self, "更新", "缺少更新模組 (update_check / update_worker)")
            return
        manifest_url = self.config.get("update_manifest_url", "").strip()
        if not manifest_url:
            QtWidgets.QMessageBox.warning(self, "更新", "config.json 缺少 update_manifest_url")
            return
        app_dir = os.path.abspath(os.path.dirname(sys.argv[0]))
        mgr = UpdateManager(manifest_url, __version__, app_dir)
        self.append_log("開始檢查更新...")
        self.progressBar.setValue(0)
        self.ui.lblSummary.setText("更新檢查中...")

        def run_fn(on_progress):
            return mgr.run_update(on_progress)

        self.update_worker = UpdateWorker(run_fn)
        self.update_worker.progressChanged.connect(self._on_update_progress)
        self.update_worker.finishedWithResult.connect(self._on_update_finished)
        self.ui.btnCheckUpdate.setEnabled(False)
        self.update_worker.start()

    def _on_update_progress(self, pct: int, text: str):
        self.progressBar.setValue(pct)
        self.ui.lblSummary.setText(text)
        self.append_log(text)

    def _on_update_finished(self, res: dict):
        self.ui.btnCheckUpdate.setEnabled(True)
        if not res.get("ok"):
            msg = res.get("error", "未知錯誤")
            self.append_log("更新失敗:" + msg)
            QtWidgets.QMessageBox.critical(self, "更新失敗", msg)
            self.progressBar.setValue(0)
            self.ui.lblSummary.setText("就緒")
            return
        if not res.get("updated"):
            self.append_log("已是最新版本")
            QtWidgets.QMessageBox.information(self, "更新", "目前已是最新版本。")
            self.progressBar.setValue(0)
            self.ui.lblSummary.setText("就緒")
            return
        latest = res.get("latest_version", "?")
        count = res.get("count", 0)
        self.append_log(f"更新完成 覆蓋 {count} 檔 -> {latest}")
        self.progressBar.setValue(100)
        self.ui.lblSummary.setText(f"更新完成 (覆蓋 {count})")
        if QtWidgets.QMessageBox.question(
            self, "更新完成",
            f"已更新到 {latest}，是否立即重啟？",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
        ) == QtWidgets.QMessageBox.StandardButton.Yes:
            self.restart_app()

    def restart_app(self):
        try:
            QtCore.QProcess.startDetached(sys.executable, sys.argv)
        except:
            pass
        self.close()

    # --- Woo 設定操作 ---
    def on_woo_save(self):
        self.config.set("woo_url", self.ui.editWooUrl.text().strip())
        self.config.set("woo_consumer_key", self.ui.editWooCK.text().strip())
        self.config.set("woo_consumer_secret", self.ui.editWooCS.text().strip())
        self.config.set("woo_fee_product_id", int(self.ui.spinWooFeeProduct.value()))
        self.config.set("woo_batch_size", int(self.ui.spinWooBatch.value()))
        self.config.set("woo_timeout", int(self.ui.spinWooTimeout.value()))
        self.config.set("woo_remote_dup_scan_limit", int(self.ui.spinWooRemoteScan.value()))
        self.config.set("woo_parallel_workers", int(self.ui.spinWooWorkers.value()))
        self.config.set("woo_test_mode", self.ui.chkWooTestMode.isChecked())
        self.config.set("woo_set_created_time", self.ui.chkWooSetCreated.isChecked())
        self.append_log("Woo 設定已儲存")
        QtWidgets.QMessageBox.information(self, "Woo 設定", "已儲存。")

    def on_woo_test(self):
        client = self.build_woo_client()
        if not client.base_url:
            QtWidgets.QMessageBox.warning(self, "測試失敗", "請填寫 URL")
            return
        r = client.test_connection()
        if r["ok"]:
            self.ui.lblWooStatus.setText(f"連線成功 (HTTP {r['status']})")
        else:
            self.ui.lblWooStatus.setText(f"失敗: {r['message']}")
        self.append_log(f"Woo 測試:{r}")

    # --- 上傳並建立訂單（由勾選觸發） ---
    def _task_upload_orders(self, all_records):
        client = self.build_woo_client()
        if not client.base_url or not client.ck or not client.cs:
            QtWidgets.QMessageBox.warning(self, "設定缺失", "URL 或 Key/Secret 未填")
            return

        client.preload_remote_fingerprints()
        fee_rate = self.config.get("platform_fee_rate", 0.07)
        fee_product_id = int(self.config.get("woo_fee_product_id", 30977))

        to_upload = []
        skip_remote = 0
        for rec in all_records:
            fp = client._fingerprint(rec)
            if fp in client.remote_fingerprints:
                skip_remote += 1
                self.db.update_woo_result(
                    rec.get("id"),
                    status="test" if client.test_mode else "success",
                    order_id=None,
                    error="remote_dup",
                    payload={},
                    fingerprint=fp,
                    attempts=0
                )
                continue
            to_upload.append(rec)

        self.append_log(f"[建立訂單] 待上傳:{len(to_upload)} 遠端跳過:{skip_remote}")
        if not to_upload:
            QtWidgets.QMessageBox.information(self, "結果", "全部遠端已存在，無需建立。")
            return

        workers = max(1, int(self.config.get("woo_parallel_workers", 6)))
        self.append_log(f"[建立訂單] 開始並行 workers={workers} test_mode={client.test_mode}")

        success = fail = 0
        failures = []
        total = len(to_upload)
        self.progressBar.setValue(0)

        def task_fn(rec):
            return client.create_order_full(
                rec,
                fee_rate=fee_rate,
                fee_product_id=fee_product_id,
                product_display=PRODUCT_CN_MAP.get(rec.get("product_type", "game_currency"), "遊戲幣")
            )

        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_map = {ex.submit(task_fn, rec): rec for rec in to_upload}
            completed = 0
            for fut in as_completed(future_map):
                result = fut.result()
                rec = future_map[fut]
                fp = result.get("fingerprint")
                if result["ok"]:
                    status = "test" if client.test_mode else "success"
                    self.db.update_woo_result(
                        rec.get("id"), status=status,
                        order_id=str(result.get("order_id")),
                        error=None, payload=result.get("payload"),
                        fingerprint=fp, attempts=result.get("attempts", 1)
                    )
                    success += 1
                    client.remote_fingerprints.add(fp)
                else:
                    self.db.update_woo_result(
                        rec.get("id"), status="error",
                        order_id=None, error=result.get("error"),
                        payload=result.get("payload"),
                        fingerprint=fp, attempts=result.get("attempts", 1)
                    )
                    fail += 1
                    failures.append(f"ID:{rec.get('id')} err:{result.get('error')}")
                completed += 1
                pct = int(completed / total * 100)
                self.progressBar.setValue(pct)
                self.ui.lblSummary.setText(
                    f"[訂單建立] {completed}/{total} 成:{success} 失:{fail} 跳遠:{skip_remote}"
                )
                QtWidgets.QApplication.processEvents()

        self.progressBar.setValue(100)
        summary = f"[訂單建立] 完成 成功:{success} 失敗:{fail} 跳遠:{skip_remote}"
        self.append_log(summary)
        if fail > 0:
            msg = summary + "\n前幾筆錯誤:\n" + "\n".join(failures[:10])
            QtWidgets.QMessageBox.warning(self, "訂單建立完成(含失敗)", msg)
        else:
            extra = ""
            if skip_remote > 0:
                extra = f"\n{skip_remote} 筆遠端已存在未重複建立。"
            QtWidgets.QMessageBox.information(self, "訂單建立完成", summary + extra)

    # --- 手續費設定 ---
    def on_save_settings(self):
        new_rate = self.ui.spinFeeRate.value() / 100.0
        self.config.set("platform_fee_rate", new_rate, autosave=True)
        self.append_log(f"平台手續費率更新 {new_rate*100:.2f}%")
        QtWidgets.QMessageBox.information(self, "設定已儲存", f"平台手續費率：{new_rate*100:.2f}%")

    def on_reset_settings(self):
        self.ui.spinFeeRate.setValue(7.0)
        self.config.set("platform_fee_rate", 0.07, autosave=True)
        self.append_log("平台手續費率重設為 7%")
        QtWidgets.QMessageBox.information(self, "重設", "已恢復 7%")

    # --- 其它 UI 行為 ---
    def on_product_mode_changed(self):
        self.ui.cbSingleProduct.setEnabled(self.ui.cbProductMode.currentText() == "單一商品")

    def center_window(self):
        screen = QtGui.QGuiApplication.primaryScreen()
        if not screen:
            return
        geo = screen.availableGeometry()
        frame = self.frameGeometry()
        frame.moveCenter(geo.center())
        self.move(frame.topLeft())

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event):
        added = 0
        for u in event.mimeData().urls():
            p = u.toLocalFile()
            if os.path.isfile(p) and p.lower().endswith((".csv", ".xlsx", ".xls")):
                self.ui.listFiles.addItem(p)
                added += 1
        if added:
            self.append_log(f"拖入 {added} 檔案")
            self.set_summary(f"已加入 {added} 檔案")

    def collect_files(self):
        return [
            self.ui.listFiles.item(i).text()
            for i in range(self.ui.listFiles.count())
            if os.path.isfile(self.ui.listFiles.item(i).text())
        ]

    def append_log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.ui.txtLog.appendPlainText(f"[{ts}] {msg}")
        self.ui.txtLog.verticalScrollBar().setValue(self.ui.txtLog.verticalScrollBar().maximum())

    def set_summary(self, text):
        self.ui.lblSummary.setText(text)

    def update_status(self):
        try:
            rows = self.db.count_rows()
        except:
            rows = 0
        self.ui.lblStatus.setText(f"DB:{rows} | v{__version__}")
        self.progressBar.setValue(0)
        self.set_summary("就緒")

    def on_toggle_auto_clear(self, state):
        self.config.set("discard_db_each_start", state == QtCore.Qt.CheckState.Checked)
        self.append_log(f"啟動清空={state == QtCore.Qt.CheckState.Checked}")

    def on_clear_now(self):
        if QtWidgets.QMessageBox.question(
            self,
            "確認",
            "立即清空交易紀錄？",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
        ) == QtWidgets.QMessageBox.StandardButton.Yes:
            clear_all_transactions(self.db_path)
            self.update_status()
            self.append_log("資料庫已清空")

    def show_list_context_menu(self, pos):
        menu = QtWidgets.QMenu(self)
        act_remove = menu.addAction("移除選取")
        act_clear = menu.addAction("清空列表")
        action = menu.exec(self.ui.listFiles.mapToGlobal(pos))
        if action == act_remove:
            self.remove_selected_files()
        elif action == act_clear:
            self.clear_file_list()

    def remove_selected_files(self):
        items = self.ui.listFiles.selectedItems()
        c = 0
        for it in items:
            self.ui.listFiles.takeItem(self.ui.listFiles.row(it))
            c += 1
        self.set_summary(f"移除 {c} 檔案")
        self.append_log(f"移除 {c} 檔案")

    def clear_file_list(self):
        self.ui.listFiles.clear()
        self.set_summary("列表已清空")
        self.append_log("檔案列表已清空")

    def rescan_input_folder(self):
        inp = ROOT_DIR / "input"
        os.makedirs(inp, exist_ok=True)
        existing = set(self.collect_files())
        added = 0
        for fn in os.listdir(inp):
            fp = str(inp / fn)
            if (
                os.path.isfile(fp)
                and fp.lower().endswith((".csv", ".xlsx", ".xls"))
                and fp not in existing
            ):
                self.ui.listFiles.addItem(fp)
                added += 1
        self.set_summary(f"掃描新增 {added}")
        self.append_log(f"掃描 input 新增 {added} 檔案")

    def open_file_location(self, item):
        open_dir(os.path.dirname(item.text()))

    def open_output_folder(self):
        open_dir(str(output_root(ROOT_DIR)))

    def refresh_preview(self):
        sel = self.ui.listFiles.selectedItems()
        if not sel:
            self.ui.tablePreview.clear()
            self.ui.tablePreview.setRowCount(0)
            self.ui.tablePreview.setColumnCount(0)
            return
        f = sel[0].text()
        try:
            recs = self.parser.parse_file(f)
        except Exception as e:
            self.append_log(f"預覽解析失敗:{e}")
            return
        show = recs[:20]
        if not show:
            self.ui.tablePreview.clear()
            return
        cols = ["direction", "amount", "customer_name", "apply_time", "finish_time", "note", "order_no"]
        self.ui.tablePreview.setColumnCount(len(cols))
        self.ui.tablePreview.setHorizontalHeaderLabels(cols)
        self.ui.tablePreview.setRowCount(len(show))
        for r, rec in enumerate(show):
            for c, col in enumerate(cols):
                item = QtWidgets.QTableWidgetItem(str(rec.get(col, "")))
                item.setFlags(item.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
                self.ui.tablePreview.setItem(r, c, item)
        self.ui.tablePreview.resizeColumnsToContents()

    def assign_product_types(self, records):
        mode = self.ui.cbProductMode.currentText()
        if mode == "單一商品":
            code = PRODUCT_CODE_MAP[self.ui.cbSingleProduct.currentText()]
            for r in records:
                r["product_type"] = code
        else:
            for r in records:
                r["product_type"] = random.choice(ALL_PRODUCT_CODES)

    def run_selected_tasks(self):
        files = self.collect_files()
        if not files:
            QtWidgets.QMessageBox.information(self, "沒有檔案", "請先加入檔案")
            return

        tasks = []
        if getattr(self.ui, "chkTaskReport", None) and self.ui.chkTaskReport.isChecked():
            tasks.append("report")
        if getattr(self.ui, "chkTaskImages", None) and self.ui.chkTaskImages.isChecked():
            tasks.append("images")
        if getattr(self.ui, "chkTaskWoo", None) and self.ui.chkTaskWoo.isChecked():
            tasks.append("woo_format")
        if getattr(self.ui, "chkTaskUploadOrders", None) and self.ui.chkTaskUploadOrders.isChecked():
            tasks.append("woo_upload")

        if not tasks:
            QtWidgets.QMessageBox.information(self, "未選任務", "請勾選任務")
            return

        fee_rate = self.config.get("platform_fee_rate", 0.07)
        all_records = []
        for f in files:
            try:
                recs = self.parser.parse_file(f)
                all_records.extend(recs)
            except Exception as e:
                self.append_log(f"{os.path.basename(f)} 解析失敗:{e}")
        if not all_records:
            QtWidgets.QMessageBox.information(self, "無資料", "解析為空")
            return

        self.assign_product_types(all_records)
        self.append_log("商品指派完成")
        self.progressBar.setValue(10)

        if "report" in tasks:
            try:
                ins, _dup = self.db.insert_records(all_records, dedup=True)
                conn = sqlite3.connect(self.db_path)
                rpt_dir = match_report_dir(output_root(ROOT_DIR))
                paths = generate_match_reports(conn, str(rpt_dir), fee_rate=fee_rate)
                conn.close()
                self.append_log(f"媒合報表完成 (+{ins})")
                QtWidgets.QMessageBox.information(
                    self,
                    "媒合報表",
                    f"明細：{paths['detail_xlsx']}\n彙總：{paths['summary_xlsx']}"
                )
                open_dir(str(rpt_dir))
            except Exception as e:
                self.append_log(f"媒合報表失敗:{e}")
                QtWidgets.QMessageBox.critical(self, "媒合報表失敗", str(e))
            self.update_status()
        self.progressBar.setValue(40)

        if "images" in tasks:
            grouped = {}
            last_dir = None
            for r in all_records:
                grouped.setdefault(r.get("product_type", "game_currency"), []).append(r)
            for ptype, recs in grouped.items():
                base_dir = chat_images_product_dir(output_root(ROOT_DIR), ptype)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                run_dir = os.path.join(base_dir, f"批次_{ts}")
                os.makedirs(run_dir, exist_ok=True)
                ins_records = [r for r in recs if r.get("direction") == "in"]
                outs_records = [r for r in recs if r.get("direction") == "out"]
                if ins_records:
                    generate_images_from_records(ins_records, run_dir, "入帳")
                if outs_records:
                    generate_images_from_records(outs_records, run_dir, "出帳")
                last_dir = run_dir
            self.append_log("圖片生成完成")
            if last_dir:
                open_dir(last_dir)
        self.progressBar.setValue(70)

        if "woo_format" in tasks:
            woo_dir = woo_export_dir(output_root(ROOT_DIR))
            for f in files:
                pcode = (
                    PRODUCT_CODE_MAP[self.ui.cbSingleProduct.currentText()]
                    if self.ui.cbProductMode.currentText() == "單一商品"
                    else random.choice(ALL_PRODUCT_CODES)
                )
                base = os.path.splitext(os.path.basename(f))[0]
                out_csv = str(woo_dir / f"{base}_{pcode}_woo.csv")
                try:
                    bank_convert(f, out_csv, fee_rate=fee_rate, product_type=pcode)
                except Exception as e:
                    self.append_log(f"{base} 匯出錯誤:{e}")
            self.append_log("轉換成可匯入網站格式完成")
            open_dir(str(woo_dir))

        if "woo_upload" in tasks:
            # 若未執行 report，仍需入庫確保有 id
            if "report" not in tasks:
                inserted, _dup = self.db.insert_records(all_records, dedup=True)
                self.append_log(f"為訂單上傳先入庫 新增:{inserted}")
            self._task_upload_orders(all_records)

        self.progressBar.setValue(100)
        self.append_log("全部完成")
        self.set_summary("完成")
        self.write_log_file()

    def write_log_file(self):
        base = logs_dir(output_root(ROOT_DIR))
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = base / f"執行日誌_{stamp}.log"
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("=== UI Log Snapshot ===\n")
                f.write(self.ui.txtLog.toPlainText())
            self.append_log(f"日誌寫入:{path.name}")
        except Exception as e:
            self.append_log(f"日誌寫入失敗:{e}")

    def closeEvent(self, event: QtGui.QCloseEvent):
        try:
            self.run_logger.close()
        except:
            pass
        release_lock()
        super().closeEvent(event)


def start_app():
    cfg_path = get_config_path()
    config = ConfigManager(str(cfg_path))
    lock_cfg = {
        "disable_single_instance": config.get("disable_single_instance", False),
        "lock_strategy": config.get("lock_strategy", "auto"),
        "single_instance_mutex_name": config.get("single_instance_mutex_name", "ExcelAutoAppSingletonMutex"),
        "single_instance_max_age_hours": config.get("single_instance_max_age_hours", 12)
    }
    ok, reason = acquire_lock(lock_cfg)
    if not ok:
        app = QtWidgets.QApplication(sys.argv)
        QtWidgets.QMessageBox.warning(
            None,
            "已在執行",
            f"偵測到已有程式或殘留鎖 (reason={reason})。\n若確認無執行中實例，可加 --force-unlock 再試。"
        )
        sys.exit(0)

    ensure_base_dirs()
    if hasattr(QtCore.Qt.ApplicationAttribute, "AA_EnableHighDpiScaling"):
        QtCore.QCoreApplication.setAttribute(QtCore.Qt.ApplicationAttribute.AA_EnableHighDpiScaling)
    if hasattr(QtCore.Qt.ApplicationAttribute, "AA_UseHighDpiPixmaps"):
        QtCore.QCoreApplication.setAttribute(QtCore.Qt.ApplicationAttribute.AA_UseHighDpiPixmaps)

    app = QtWidgets.QApplication(sys.argv)
    icon_ico = asset_path("icons", "app.ico")
    icon_png = asset_path("icons", "app.png")
    if icon_ico.exists():
        app.setWindowIcon(QtGui.QIcon(str(icon_ico)))
    elif icon_png.exists():
        app.setWindowIcon(QtGui.QIcon(str(icon_png)))

    w = MainWindow(config)
    w.show()
    code = app.exec()
    release_lock()
    sys.exit(code)


if __name__ == "__main__":
    start_app()
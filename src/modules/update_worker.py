from PyQt6 import QtCore
from typing import Dict, Any, Callable

class UpdateWorker(QtCore.QThread):
    progressChanged = QtCore.pyqtSignal(int, str)   # (percent, text)
    finishedWithResult = QtCore.pyqtSignal(dict)    # result dict

    def __init__(self, run_fn: Callable[[Callable[[int, str], None]], Dict[str, Any]]):
        super().__init__()
        self._run_fn = run_fn

    def _emit_progress(self, pct: int, text: str):
        try:
            self.progressChanged.emit(pct, text)
        except:
            pass

    def run(self):
        res = {}
        try:
            res = self._run_fn(self._emit_progress)
        except Exception as e:
            res = {"ok": False, "error": f"更新執行緒錯誤: {e}"}
        self.finishedWithResult.emit(res)
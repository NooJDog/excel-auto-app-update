import os, sys, json, time, traceback, datetime
from typing import Optional

class RunLogger:
    def __init__(self, base_dir: str = "logs", run_id: Optional[str] = None):
        self.start_time = time.time()
        self.run_id = run_id or datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.exec_log_path = os.path.join(self.base_dir, f"run_{self.run_id}.log")
        self.error_log_path = os.path.join(self.base_dir, f"errors_{self.run_id}.log")
        self.structured_path = os.path.join(self.base_dir, f"struct_{self.run_id}.jsonl")
        self.crash_log_path = os.path.join(self.base_dir, f"crash_{self.run_id}.log")
        self._exec_fp = open(self.exec_log_path, "a", encoding="utf-8")
        self._err_fp = open(self.error_log_path, "a", encoding="utf-8")
        self._struct_fp = open(self.structured_path, "a", encoding="utf-8")
        sys.excepthook = self._excepthook

    def _timestamp(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def info(self, msg: str):
        line = f"[{self._timestamp()}] {msg}"
        self._exec_fp.write(line + "\n"); self._exec_fp.flush()
        self._struct_fp.write(json.dumps({"ts": datetime.datetime.utcnow().isoformat()+"Z",
                                          "level":"INFO","msg":msg})+"\n"); self._struct_fp.flush()

    def warn(self, msg: str):
        line = f"[{self._timestamp()}] WARNING {msg}"
        self._exec_fp.write(line + "\n"); self._exec_fp.flush()
        self._struct_fp.write(json.dumps({"ts": datetime.datetime.utcnow().isoformat()+"Z",
                                          "level":"WARN","msg":msg})+"\n"); self._struct_fp.flush()

    def error(self, msg: str, exc: Optional[Exception]=None):
        line = f"[{self._timestamp()}] ERROR {msg}"
        self._exec_fp.write(line + "\n"); self._exec_fp.flush()
        stack = ""
        if exc:
            stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self._err_fp.write(line + "\n")
        if stack:
            self._err_fp.write(stack + "\n")
        self._err_fp.flush()
        self._struct_fp.write(json.dumps({"ts": datetime.datetime.utcnow().isoformat()+"Z",
                                          "level":"ERROR","msg":msg,"stack":stack})+"\n"); self._struct_fp.flush()

    def structured(self, event: str, **fields):
        payload = {"ts": datetime.datetime.utcnow().isoformat()+"Z",
                   "level":"INFO","event":event}
        payload.update(fields)
        self._struct_fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._struct_fp.flush()

    def snapshot_requirements(self, outfile: Optional[str]=None):
        """
        避免在 frozen exe 環境觸發自我啟動循環：直接跳過。
        """
        if getattr(sys, "frozen", False):
            self.info("Skip requirements snapshot in frozen build")
            return
        import subprocess
        path = outfile or os.path.join(self.base_dir, f"requirements_snapshot_{self.run_id}.txt")
        try:
            result = subprocess.run([sys.executable,"-m","pip","freeze"], capture_output=True, text=True, timeout=30)
            with open(path,"w",encoding="utf-8") as f:
                f.write(result.stdout)
            self.info(f"Requirements snapshot saved: {path}")
        except Exception as e:
            self.error("Save requirements snapshot failed", e)

    def close(self):
        elapsed = time.time() - self.start_time
        self.info(f"END run_id={self.run_id} elapsed={elapsed:.2f}s")
        for fp in (self._exec_fp, self._err_fp, self._struct_fp):
            try: fp.close()
            except: pass

    def _excepthook(self, etype, value, tb):
        stack = "".join(traceback.format_exception(etype, value, tb))
        with open(self.crash_log_path,"a",encoding="utf-8") as f:
            f.write(f"[{self._timestamp()}] CRASH {value}\n")
            f.write(stack + "\n")
        self.error(f"Unhandled exception: {value}", value)

def init_logger():
    return RunLogger()
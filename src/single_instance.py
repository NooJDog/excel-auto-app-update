import os, sys, json, time, tempfile, ctypes

# -------------- 設定常數 --------------
LOCK_FILENAME = "excel_auto_app_instance.lock"
LOCK_PATH = os.path.join(tempfile.gettempdir(), LOCK_FILENAME)

# 預設過期時數（可由外部傳入覆蓋）
DEFAULT_MAX_AGE_HOURS = 12

# -------------- Mutex (Windows) --------------
class WindowsMutex:
    def __init__(self, name: str):
        self.name = name
        self.handle = None
        self.last_error = 0

    def acquire(self):
        # 建立或取得命名 Mutex
        CreateMutex = ctypes.windll.kernel32.CreateMutexW
        GetLastError = ctypes.windll.kernel32.GetLastError
        self.handle = CreateMutex(None, False, self.name)
        if not self.handle:
            self.last_error = GetLastError()
            return False
        self.last_error = GetLastError()
        # ERROR_ALREADY_EXISTS = 183
        if self.last_error == 183:
            return False
        return True

    def release(self):
        if self.handle:
            ctypes.windll.kernel32.CloseHandle(self.handle)
            self.handle = None

def _now(): return int(time.time())

def _read_lock_file():
    if not os.path.exists(LOCK_PATH):
        return None
    try:
        with open(LOCK_PATH,"r",encoding="utf-8") as f:
            txt=f.read().strip()
        if not txt: return None
        if txt.startswith("{"):
            return json.loads(txt)
        # 舊格式只有 pid
        return {"pid": int(txt), "start_time": None}
    except Exception:
        return None

def _is_pid_alive(pid:int)->bool:
    if pid<=0: return False
    if sys.platform.startswith("win"):
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION=0x1000
        handle=ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    else:
        try:
            os.kill(pid,0)
            return True
        except Exception:
            return False

def _write_lock_file():
    data={
        "pid": os.getpid(),
        "start_time": _now(),
        "exe": sys.executable,
        "argv": sys.argv
    }
    tmp=LOCK_PATH+".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(data,f)
    os.replace(tmp,LOCK_PATH)

def _remove_lock_file():
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except Exception:
        pass

def acquire_lock(config:dict=None):
    """
    config 可含：
      disable_single_instance
      lock_strategy: auto | mutex | file
      single_instance_mutex_name
      single_instance_max_age_hours
    回傳 (acquired:bool, reason:str)
    reason in {"ok","disabled","already_running","error"}
    """
    cfg=config or {}
    if os.environ.get("SINGLE_INSTANCE_FORCE_BYPASS")=="1":
        return True,"bypass_env"

    if "--force-unlock" in sys.argv:
        _remove_lock_file()

    if cfg.get("disable_single_instance", False):
        return True,"disabled"

    strategy=cfg.get("lock_strategy","auto")
    max_age_hours=cfg.get("single_instance_max_age_hours", DEFAULT_MAX_AGE_HOURS)
    mutex_name=cfg.get("single_instance_mutex_name","ExcelAutoAppSingletonMutex")

    if strategy not in ("auto","mutex","file"):
        strategy="auto"

    # Windows Mutex 優先
    if sys.platform.startswith("win") and strategy in ("auto","mutex"):
        try:
            wm=WindowsMutex(mutex_name)
            ok=wm.acquire()
            if ok:
                # 仍寫檔案鎖（提供崩潰恢復資訊）
                _write_lock_file()
                return True,"ok"
            else:
                # 仍檢查檔案鎖是否 stale
                data=_read_lock_file()
                if data:
                    pid=data.get("pid", -1)
                    st=data.get("start_time")
                    age_hours=0
                    if st:
                        age_hours=( _now()-int(st) ) / 3600.0
                    if not _is_pid_alive(pid) or (st and age_hours>max_age_hours):
                        _remove_lock_file()
                        _write_lock_file()
                        return True,"ok_recover"
                return False,"already_running"
        except Exception as e:
            # 回退使用檔案鎖
            pass

    # 非 Windows 或策略=file
    data=_read_lock_file()
    if not data:
        _write_lock_file()
        return True,"ok"
    pid=data.get("pid",-1)
    st=data.get("start_time")
    age_hours=0
    if st:
        age_hours=( _now()-int(st) )/3600.0

    if not _is_pid_alive(pid) or (st and age_hours>max_age_hours):
        _remove_lock_file()
        _write_lock_file()
        return True,"ok_recover"

    return False,"already_running"

def release_lock():
    _remove_lock_file()

def force_release():
    _remove_lock_file()

if __name__=="__main__":
    # 測試： python single_instance.py
    got, reason = acquire_lock({})
    print("acquire:", got, "reason:", reason)
    if got:
        input("Locked. Press Enter to release...")
        release_lock()
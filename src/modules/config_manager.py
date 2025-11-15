import os, json, threading

_DEFAULT_CONFIG = {
    "db_path": "db/transactions.db",
    "discard_db_each_start": True,
    "use_memory_db": False,
    "platform_fee_rate": 0.07,

    "enable_woo_sync": True,
    "woo_url": "",
    "woo_consumer_key": "",
    "woo_consumer_secret": "",
    "woo_fee_product_id": 30977,
    "woo_test_mode": True,
    "woo_batch_size": 200,
    "woo_timeout": 15,

    "disable_single_instance": False,
    "lock_strategy": "auto",
    "single_instance_mutex_name": "ExcelAutoAppSingletonMutex",
    "single_instance_max_age_hours": 12,

    "woo_remote_dup_scan_limit": 200,
    "woo_set_created_time": True,
    "woo_parallel_workers": 6,

    # 新增：更新檢查的 manifest URL（請換成你實際 Raw 連結）
    "update_manifest_url": "https://raw.githubusercontent.com/NooJDog/excel-auto-app-update/main/manifest.json"
}

class ConfigManager:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        if not os.path.exists(path):
            self._data = _DEFAULT_CONFIG.copy()
            self._write()
        else:
            try:
                with open(path,"r",encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = _DEFAULT_CONFIG.copy()
                self._write()
        changed=False
        for k,v in _DEFAULT_CONFIG.items():
            if k not in self._data:
                self._data[k]=v; changed=True
        if changed: self._write()

    def _write(self):
        tmp=self.path + ".tmp"
        with self._lock:
            with open(tmp,"w",encoding="utf-8") as f:
                json.dump(self._data,f,indent=2,ensure_ascii=False)
            os.replace(tmp,self.path)

    def save(self): self._write()
    def get(self,key,default=None): return self._data.get(key,default)
    def set(self,key,val,autosave=True):
        self._data[key]=val
        if autosave: self._write()
    def all(self): return dict(self._data)
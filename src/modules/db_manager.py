import sqlite3, hashlib, datetime, os, json

REQUIRED_COLS = [
    "direction","amount","raw_amount","customer_name","apply_time","finish_time","note",
    "source_file","linkage_id","order_no","status","match_time","remaining_amount","dedup_hash",
    "phone","pay_code","invoice_flag","invoice_amount","bank_account","_read_mode","created_at",
    "product_type",
    "woo_order_id","woo_sync_status","woo_sync_error","woo_synced_at",
    "woo_last_payload_json","woo_sync_attempts","woo_tx_fingerprint"
]

class DBManager:
    def __init__(self, db_path):
        self.db_path=db_path
        os.makedirs(os.path.dirname(db_path),exist_ok=True)
        # check_same_thread=False 允許 ThreadPool 讀取
        self.conn=sqlite3.connect(db_path, check_same_thread=False)
        self._ensure()

    def _ensure(self):
        c=self.conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        c.execute("PRAGMA table_info(transactions)")
        existing=[r[1] for r in c.fetchall()]
        for col in REQUIRED_COLS:
            if col not in existing:
                try: c.execute(f"ALTER TABLE transactions ADD COLUMN {col} TEXT")
                except sqlite3.OperationalError: pass
        c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_dedup ON transactions(dedup_hash)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_apply ON transactions(apply_time)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_woo_sync ON transactions(woo_sync_status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_woo_fp ON transactions(woo_tx_fingerprint)")
        self.conn.commit()

    def count_rows(self):
        c=self.conn.cursor()
        try:
            c.execute("SELECT COUNT(*) FROM transactions")
            return c.fetchone()[0]
        except:
            return 0

    def _dedup_sig(self, direction, order_no, customer, amount, apply_time, finish_time, source):
        if order_no and not order_no.startswith("FALLBACK_"):
            return hashlib.sha256(order_no.encode()).hexdigest()
        date_part=""
        for t in [apply_time, finish_time]:
            if t:
                date_part=str(t)[:10]; break
        base=f"{direction}|{customer}|{amount}|{date_part}|{source}"
        return hashlib.sha256(base.encode()).hexdigest()

    def insert_records(self, records, dedup=True):
        """
        將新插入的資料庫 id 回填到 record["id"] 供後續同步使用。
        """
        c=self.conn.cursor()
        new=dup=0
        for r in records:
            direction=r.get("direction","in")
            product_type=r.get("product_type","game_currency")
            raw=r.get("raw_amount_number") or r.get("amount") or 0
            try: raw=float(str(raw).replace(",",""))
            except: raw=0.0
            amount=int(abs(raw))
            customer=r.get("nickname") or r.get("customer_name") or ""
            apply=r.get("apply_time") or r.get("time") or ""
            finish=r.get("finish_time") or ""
            note=r.get("note") or ""
            source=r.get("source_file") or ""
            order_no=r.get("order_no") or ""
            created=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            sig=self._dedup_sig(direction, order_no, customer, amount, apply, finish, source)

            if dedup:
                if order_no:
                    c.execute("SELECT 1 FROM transactions WHERE order_no=?",(order_no,))
                    if c.fetchone(): dup+=1; continue
                c.execute("SELECT 1 FROM transactions WHERE dedup_hash=?",(sig,))
                if c.fetchone(): dup+=1; continue

            c.execute("""
                INSERT INTO transactions
                (direction,amount,raw_amount,customer_name,apply_time,finish_time,note,source_file,
                 linkage_id,order_no,status,match_time,remaining_amount,dedup_hash,
                 phone,pay_code,invoice_flag,invoice_amount,bank_account,_read_mode,created_at,
                 product_type, woo_order_id, woo_sync_status, woo_sync_error, woo_synced_at,
                 woo_last_payload_json, woo_sync_attempts, woo_tx_fingerprint)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?)
            """,(direction,amount,raw,customer,apply,finish,note,source,
                 "",order_no,None,None,None,sig,
                 r.get("phone") or "",r.get("pay_code") or "",r.get("invoice_flag") or "",
                 r.get("invoice_amount") or 0,r.get("bank_account") or "",r.get("_read_mode") or "",created,
                 product_type, None, None, None, None,
                 None, None, None))
            new+=1
            r["id"]=c.lastrowid  # 回填 id

        self.conn.commit()
        return new,dup

    def fingerprint_exists_success(self, fingerprint:str)->bool:
        c=self.conn.cursor()
        c.execute("SELECT 1 FROM transactions WHERE woo_tx_fingerprint=? AND woo_sync_status='success' LIMIT 1",(fingerprint,))
        return c.fetchone() is not None

    def update_woo_result(self, record_id:int, status:str, order_id:str=None,
                          error:str=None, payload:dict=None, fingerprint:str=None,
                          attempts:int=1):
        if record_id is None:
            return  # 安全防護，避免 None 破壞資料
        ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pj=None
        if payload is not None:
            try: pj=json.dumps(payload,ensure_ascii=False)[:4000]
            except: pj=None
        self.conn.execute("""
            UPDATE transactions
            SET woo_sync_status=?,
                woo_order_id=?,
                woo_sync_error=?,
                woo_synced_at=?,
                woo_last_payload_json=?,
                woo_sync_attempts=?,
                woo_tx_fingerprint=COALESCE(woo_tx_fingerprint,?)
            WHERE id=?
        """,(status,order_id,error,ts,pj,attempts,fingerprint,record_id))
        self.conn.commit()
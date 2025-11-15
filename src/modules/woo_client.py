import requests, time, hashlib, json, datetime, re, random
from typing import Dict, Any, Set

RETRY_STATUS_CODES = {429}
SERVER_ERROR_PREFIX = 500
MAX_RETRIES = 3
BACKOFF_SECONDS = [1,2,4]
EMAIL_DOMAINS = ["gmail.com","yahoo.com","outlook.com","hotmail.com"]

def normalize_amount(val) -> int:
    if val is None: return 0
    s = str(val).strip()
    if not s: return 0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True; s = s[1:-1]
    s = (s.replace(",", "").replace("，","").replace("$","")
           .replace("NT$","").replace("元","").replace(" ",""))
    m = re.search(r'-?\d+(?:\.\d+)?', s)
    if not m: return 0
    v = float(m.group(0))
    if neg: v = -v
    return int(v)

def generate_realistic_email(record:Dict[str,Any])->str:
    base_src = (record.get("order_no") or record.get("customer_name") or "").lower()
    seed = re.sub(r'[^a-z0-9]', '', base_src)
    if len(seed) < 6:
        seed += ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(6-len(seed)))
    letters = [c for c in seed if c.isalpha()]
    digits  = [c for c in seed if c.isdigit()]
    if not letters:
        letters = [random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(3)]
    if not digits:
        digits = [str(random.randint(0,9)) for _ in range(3)]
    out=[]; li=di=0
    while li < len(letters) or di < len(digits):
        if li < len(letters):
            out.append(letters[li]); li+=1
        if di < len(digits):
            out.append(digits[di]); di+=1
        if len(out) >= 12: break
    local = ''.join(out)
    return f"{local}@{random.choice(EMAIL_DOMAINS)}"

class WooClient:
    def __init__(self, base_url:str, consumer_key:str, consumer_secret:str,
                 timeout:int=15, test_mode:bool=True, logger=None,
                 remote_dup_scan_limit:int=200, set_created_time:bool=True):
        self.base_url=base_url.rstrip("/")
        self.ck=consumer_key.strip()
        self.cs=consumer_secret.strip()
        self.timeout=timeout
        self.test_mode=test_mode
        self.logger=logger
        self.session=requests.Session()
        self.remote_dup_scan_limit=remote_dup_scan_limit
        self.set_created_time=set_created_time
        self.remote_fingerprints:Set[str]=set()
        self.remote_loaded=False

    def _log(self, level:str, msg:str):
        if self.logger:
            getattr(self.logger, level)(msg)

    def _orders_endpoint(self)->str:
        return f"{self.base_url}/wp-json/wc/v3/orders"

    def _fallback_orders_endpoint(self)->str:
        return f"{self.base_url}/?rest_route=/wc/v3/orders"

    def test_connection(self)->Dict[str,Any]:
        url=self._orders_endpoint()+"?per_page=1"
        try:
            r=self.session.get(url, auth=(self.ck,self.cs), timeout=self.timeout)
            if r.status_code==200: return {"ok":True,"status":r.status_code,"message":"連線成功"}
            if r.status_code==404:
                fr=self._fallback_orders_endpoint()+"?per_page=1"
                r2=self.session.get(fr, auth=(self.ck,self.cs), timeout=self.timeout)
                if r2.status_code==200: return {"ok":True,"status":r2.status_code,"message":"主端點 404, fallback 成功"}
                return {"ok":False,"status":r.status_code,"message":r.text}
            return {"ok":False,"status":r.status_code,"message":r.text}
        except Exception as e:
            return {"ok":False,"status":0,"message":repr(e)}

    def _fingerprint(self, record:Dict[str,Any])->str:
        base=f"{record.get('direction')}|{record.get('order_no')}|{record.get('apply_time')}|{record.get('amount')}|{record.get('customer_name')}"
        return hashlib.sha256(base.encode()).hexdigest()

    def _percent_two_dec(self, fee_rate:float)->str:
        return f"{fee_rate*100:.2f}%"

    def _parse_iso(self, dt_str:str)->str:
        if not dt_str: return ""
        dt_str=dt_str.strip()
        if not dt_str: return ""
        fmts=[
            "%Y-%m-%d %H:%M:%S","%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d %H:%M","%Y/%m/%d %H:%M",
            "%Y-%m-%d","%Y/%m/%d"
        ]
        for f in fmts:
            try:
                dt=datetime.datetime.strptime(dt_str,f)
                from datetime import timezone, timedelta
                dt=dt.replace(tzinfo=timezone(timedelta(hours=8)))
                return dt.isoformat()
            except:
                continue
        return dt_str  # 原樣保留（MU-Plugin重試解析）

    def preload_remote_fingerprints(self):
        if self.remote_loaded: return
        self.remote_loaded=True
        limit=min(max(self.remote_dup_scan_limit,1),400)
        page=1; collected=0
        while collected<limit:
            per=min(100, limit-collected)
            url=f"{self._orders_endpoint()}?per_page={per}&page={page}&orderby=date&order=desc"
            try:
                r=self.session.get(url, auth=(self.ck,self.cs), timeout=self.timeout)
            except Exception as e:
                self._log("warn",f"[Woo preload] 連線失敗: {e}"); break
            if r.status_code!=200:
                self._log("warn",f"[Woo preload] HTTP {r.status_code} {r.text[:200]}"); break
            try: orders=r.json()
            except: break
            if not orders: break
            for o in orders:
                meta=o.get("meta_data",[])
                for m in meta:
                    if m.get("key")=="tx_fingerprint":
                        fp=m.get("value")
                        if fp: self.remote_fingerprints.add(fp)
            collected+=len(orders); page+=1
        self._log("info",f"[Woo preload] 遠端指紋載入 {len(self.remote_fingerprints)}")

    def build_order_payload(self, record:Dict[str,Any], fee_rate:float,
                            fee_product_id:int, product_display:str):
        amt_int=normalize_amount(record.get("amount"))
        if amt_int==0 and record.get("raw_amount") is not None:
            alt=normalize_amount(record.get("raw_amount"))
            if alt!=0: amt_int=alt
        fee_val=int(amt_int*fee_rate)
        percent_str=self._percent_two_dec(fee_rate)
        percent_str_with_space=f"(抽 {percent_str})"

        apply_raw=record.get("apply_time") or ""
        finish_raw=record.get("finish_time") or ""
        apply_iso=self._parse_iso(apply_raw)
        finish_iso=self._parse_iso(finish_raw)
        order_no=record.get("order_no") or ""
        status_src=record.get("status_source")
        product_display=product_display

        note=f"{fee_val}(平台手續費) = {amt_int}(交易金額)*{percent_str} {percent_str_with_space} (商品:{product_display})"
        if order_no: note+=f" | {order_no}"
        if status_src: note+=f" ; 來源狀態:{status_src}"

        fp=self._fingerprint(record)
        email=generate_realistic_email(record)

        payload={
            "status":"completed",  # 改回直接完成
            "currency":"TWD",
            "billing":{
                "first_name":record.get("customer_name") or "買家",
                "last_name":"",
                "email":email,
                "phone":""
            },
            "line_items":[
                {
                    "product_id":fee_product_id,
                    "name":"平台手續費",
                    "quantity":1,
                    "total":str(fee_val),
                    "subtotal":str(fee_val)
                }
            ],
            "customer_note":note,
            "meta_data":[
                {"key":"source_order_no","value":order_no},
                {"key":"apply_time_raw","value":apply_raw},
                {"key":"finish_time_raw","value":finish_raw},
                {"key":"apply_time_local_iso","value":apply_iso},
                {"key":"finish_time_local_iso","value":finish_iso},
                {"key":"tx_fingerprint","value":fp},
                {"key":"local_record_id","value":str(record.get('id'))},
                {"key":"product_type","value":record.get("product_type","game_currency")},
                {"key":"platform_fee_value","value":str(fee_val)},
                {"key":"platform_fee_rate_percent","value":percent_str}
            ]
        }
        return payload, fp

    def _do_post(self, url:str, payload:Dict[str,Any]):
        try:
            r=self.session.post(url, auth=(self.ck,self.cs), json=payload, timeout=self.timeout)
        except Exception as e:
            return False, {"error":repr(e)}, 0, "exception"
        ct=r.headers.get("Content-Type","")
        text=r.text
        data=None
        if "application/json" in ct.lower():
            try: data=r.json()
            except: data=None
        else:
            try: data=json.loads(text)
            except: data=None
        ok=r.status_code in (200,201)
        reason=""
        if not ok:
            if r.status_code in RETRY_STATUS_CODES: reason="rate_limit"
            elif r.status_code>=SERVER_ERROR_PREFIX: reason="server_error"
            elif r.status_code==404: reason="not_found"
            else: reason="client_error"
        return ok,(data if data is not None else {"raw":text}), r.status_code, reason

    def _post_with_retry(self, payload:Dict[str,Any]):
        attempts=0; last_err=None; last_status=0
        url=self._orders_endpoint()
        while attempts<MAX_RETRIES:
            attempts+=1
            ok,data,status,reason=self._do_post(url,payload)
            if ok: return True,data,status,attempts
            if reason in ("rate_limit","server_error") or status==0:
                last_err=data; last_status=status
                if attempts<MAX_RETRIES:
                    time.sleep(BACKOFF_SECONDS[attempts-1]); continue
            if reason=="not_found":
                last_err=data; last_status=status
                url=self._fallback_orders_endpoint()
                continue
            last_err=data; last_status=status; break
        return False,last_err,last_status,attempts

    def create_order_full(self, record:Dict[str,Any], fee_rate:float,
                          fee_product_id:int, product_display:str)->Dict[str,Any]:
        payload, fp=self.build_order_payload(record, fee_rate, fee_product_id, product_display)
        if self.test_mode:
            fake_id=int(time.time())%100000
            self._log("info",f"[Woo TEST] fake_id={fake_id} fp={fp}")
            return {"ok":True,"order_id":fake_id,"payload":payload,"fingerprint":fp,"attempts":1,"test_mode":True}
        ok,data,status,attempts=self._post_with_retry(payload)
        if not ok:
            self._log("error",f"[Woo] create fail status={status} err={data}")
            return {"ok":False,"error":str(data),"payload":payload,"fingerprint":fp,"attempts":attempts}
        order_id=data.get("id")
        return {"ok":True,"order_id":order_id,"payload":payload,"fingerprint":fp,"attempts":attempts}
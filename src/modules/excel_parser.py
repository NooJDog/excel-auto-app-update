import os
import re
import hashlib
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd

try:
    import xlrd  # 需要 xlrd==1.2.0 以支援 .xls
except ImportError:
    xlrd = None

# 調試：設定 True 會輸出前幾筆 order_no 等
DEBUG_ORDER_NO = False

# 行判定最小非空儲存格
MIN_EFFECTIVE_CELL_PER_ROW = 1

# ---- 欄位候選 ----
ORDER_NO_CANDIDATES = [
    "商戶單號","客單號","客戶單號","玩家單號","外部單號","流水號","交易編號","訂單號",
    "訂單編號","單號","票據號碼","票號","平台單號","第三方單號",
    "reference","order_no","order number","transaction id","交易序號","交易單號"
]
APPLY_TIME_CANDIDATES  = ["申請時間","日期","交易日期","apply_time","申請日期","建立時間","建立日期","寫入時間"]
FINISH_TIME_CANDIDATES = ["完成時間","付款時間","完成時間戳","finish_time","完成日期","支付時間","媒合時間"]

IN_PLAYER_NAME_CANDS = ["玩家名","玩家","玩家名稱","會員","客戶","客戶名稱","用戶","玩家暱稱","會員名稱"]
OUT_RECIPIENT_CANDS  = ["收款人","收款名稱","收款帳戶","收款户名","戶名","姓名","收款客戶","收款方"]

MEMO_CANDIDATES = [
    "附言","備註","摘要","description","流水號","交易編號","說明",
    "備註欄","備註訊息","備註內容","備註說明","訂單狀態","佣金","手續費","商戶昵稱"
]

IN_AMOUNT_CANDIDATES  = ["實付","收入金額","收入","金額","交易金額","amount","total","入帳金額","代收金額"]
OUT_AMOUNT_CANDIDATES = ["交易金額","付款金額","金額","amount","total","代付金額","支出金額","出帳金額"]

PAYOUT_KEYWORDS_IN_FILENAME = ["出帳","代付","payout","withdraw"]

# 金額匹配正則（泛用）
AMOUNT_REGEX = re.compile(r"-?\d+(?:\.\d+)?")

# ---------------- 工具函式 ----------------
def _normalize_amount(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = (s.replace(",","").replace("，","").replace("$","")
            .replace("NT$","").replace("元","").replace(" ",""))
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    v = float(m.group(0))
    return -v if neg else v

def _find_first_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_norm = [c.strip() for c in cols]
    for cand in candidates:
        cl = cand.lower()
        for c in cols_norm:
            if cl == c.lower() or cl in c.lower():
                return c
    return None

def _fuzzy_order_col(cols: List[str]) -> Optional[str]:
    # 先找含「單」且「號」
    for c in cols:
        if "單" in c and "號" in c:
            return c
    # 次之找其它關鍵字
    for kw in ["流水","交易","訂單","reference","voucher","序號","票據"]:
        for c in cols:
            if kw in c.lower():
                return c
    return None

def _extract_order_from_text(text: Any) -> str:
    if not text:
        return ""
    s = str(text)
    for pat in [r"[A-Z0-9]{6,32}", r"\b\d{6,32}\b"]:
        m = re.search(pat, s, flags=re.I)
        if m:
            return m.group(0)
    return ""

def _fallback_order_no(apply_time, finish_time, customer, amount_int):
    date_part = ""
    for t in [apply_time, finish_time]:
        if t:
            date_part = str(t)[:10]
            break
    base = f"{date_part}|{customer}|{amount_int}"
    h = hashlib.sha256(base.encode()).hexdigest()[:10]
    return f"FALLBACK_{h}"

def detect_payout_template(cols: List[str], filename: str) -> bool:
    fn_lower = filename.lower()
    if any(kw in fn_lower for kw in PAYOUT_KEYWORDS_IN_FILENAME):
        return True
    has_receiver = any("收款人" in c or "收款" in c for c in cols)
    has_bank     = any("收款銀行" in c or "收款卡號" in c for c in cols)
    has_amount   = any("交易金額" in c or "付款金額" in c for c in cols)
    has_comm     = any("佣金" in c or "手續費" in c for c in cols)
    has_merchant = any("商戶單號" in c or "商戶昵稱" in c for c in cols)
    if (has_receiver and has_bank and has_amount) or (has_comm and has_amount) or (has_merchant and has_receiver and has_amount):
        return True
    return False

def _is_effective_row(row: pd.Series) -> bool:
    # 至少有 MIN_EFFECTIVE_CELL_PER_ROW 個非空即可
    non_empty = 0
    for v in row.values:
        sv = str(v).strip()
        if sv not in ("", "nan", "None"):
            non_empty += 1
            if non_empty >= MIN_EFFECTIVE_CELL_PER_ROW:
                return True
    return False

def _extract_amount_generic(row: pd.Series) -> Optional[float]:
    # 若找不到候選欄位，掃描所有欄位尋找第一個金額
    for v in row.values:
        if v is None:
            continue
        sv = str(v).strip()
        if not sv:
            continue
        m = AMOUNT_REGEX.search(sv.replace(",","").replace("，",""))
        if m:
            try:
                return float(m.group(0))
            except:
                pass
    return None

# ---------------- .xls 手動解析 (xlrd) ----------------
def _manual_xls_to_dataframe(path: str):
    if xlrd is None:
        return None
    try:
        wb = xlrd.open_workbook(path)
    except Exception:
        return None
    sheets = wb.sheets()
    all_rows = []
    max_cols = 0
    for sh in sheets:
        nrows = sh.nrows
        ncols = sh.ncols
        if ncols > max_cols:
            max_cols = ncols
        for r in range(nrows):
            row_vals = sh.row_values(r)
            all_rows.append(row_vals)
    if not all_rows:
        return None
    # 建欄位：若第一行太少非空，生成 col_i
    header_row = all_rows[0]
    non_empty_header = sum(str(v).strip() not in ("","nan","None") for v in header_row)
    if non_empty_header < 2:
        cols = [f"col_{i}" for i in range(len(header_row))]
        data_rows = all_rows
    else:
        cols = [str(v).strip() if str(v).strip() else f"col_{i}" for i,v in enumerate(header_row)]
        data_rows = all_rows[1:]
    # 轉成 DataFrame
    df = pd.DataFrame(data_rows, columns=cols)
    return df

# ---------------- 統一讀取入口 ----------------
def unified_read(path) -> Dict[str, Any]:
    ext = os.path.splitext(path)[1].lower()
    info = {"df": None, "mode": "", "error": ""}
    try:
        if ext == ".csv":
            for enc in ["utf-8","utf-8-sig","cp950"]:
                try:
                    info["df"] = pd.read_csv(path, encoding=enc)
                    info["mode"] = f"csv({enc})"
                    return info
                except Exception:
                    continue
            info["error"] = "csv decode fail"
            return info

        if ext == ".xlsx":
            try:
                info["df"] = pd.read_excel(path, engine="openpyxl")
                info["mode"] = "xlsx"
                return info
            except Exception as e:
                info["error"] = f"xlsx read fail:{e}"
                return info

        if ext == ".xls":
            # 1) pandas + xlrd
            try:
                df_xlrd = pd.read_excel(path, engine="xlrd")
                if df_xlrd is not None and not df_xlrd.empty:
                    info["df"] = df_xlrd
                    info["mode"] = "xls_xlrd"
                    return info
            except Exception as e:
                info["error"] = f"xls xlrd pandas fail:{e}"

            # 2) 手動 xlrd 迭代
            df_manual = _manual_xls_to_dataframe(path)
            if df_manual is not None and not df_manual.empty:
                info["df"] = df_manual
                info["mode"] = "xls_manual"
                return info

            # 3) 檢查是否 HTML 偽裝
            with open(path, "rb") as f:
                head = f.read(4096)
            txt = head.decode(errors="ignore").lower()
            if "<table" in txt or "<html" in txt:
                try:
                    tables = pd.read_html(path)
                    if tables:
                        info["df"] = tables[0]
                        info["mode"] = "xls_html_fallback"
                        return info
                except Exception as e:
                    info["error"] += f";html_fail:{e}"

            # 4) 檢查是否其實是 CSV
            if txt.count(",") >= 3:
                for enc in ["utf-8","utf-8-sig","cp950"]:
                    try:
                        df_csv_fb = pd.read_csv(path, encoding=enc)
                        if not df_csv_fb.empty:
                            info["df"] = df_csv_fb
                            info["mode"] = f"xls_csv_fallback({enc})"
                            return info
                    except Exception:
                        pass

            # 5) pandas 自動再試一次
            try:
                df_auto = pd.read_excel(path)
                if df_auto is not None and not df_auto.empty:
                    info["df"] = df_auto
                    info["mode"] = "xls_auto"
                    return info
            except Exception as e:
                info["error"] += f";xls_auto_fail:{e}"

            return info

        # 其它副檔名：嘗試 excel → csv
        try:
            info["df"] = pd.read_excel(path, engine="openpyxl")
            info["mode"] = "excel_generic"
        except Exception as eg:
            try:
                info["df"] = pd.read_csv(path, encoding="utf-8")
                info["mode"] = "csv_generic"
            except Exception as ec:
                info["error"] = f"generic_fail:{eg};csv_fail:{ec}"

    except Exception as e:
        info["error"] = str(e)
    return info

# ---------------- Parser 主類別 ----------------
class ExcelParser:
    def __init__(self, db=None, logger=None, debug_dir: Optional[Path]=None):
        self.db = db
        self.logger = logger
        self.debug_dir = debug_dir
        if self.debug_dir:
            self.debug_dir.mkdir(parents=True, exist_ok=True)

    def _log(self, msg: str):
        if self.logger:
            self.logger(msg)
        else:
            print("[ExcelParser]", msg)

    def _write_preview(self, filename: str, df: Optional[pd.DataFrame], reason: str, records_len: int):
        if not self.debug_dir:
            return
        try:
            safe = filename.replace("/", "_").replace("\\", "_")
            path = self.debug_dir / f"preview_{safe}.txt"
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"檔案: {filename}\n")
                f.write(f"原因/狀態: {reason}\n")
                f.write(f"筆數: {records_len}\n")
                if df is None:
                    f.write("DF=None\n")
                    return
                f.write(f"DataFrame shape: {df.shape}\n")
                f.write("欄位:\n")
                f.write(str(df.columns.tolist()) + "\n\n")
                f.write("前 8 列預覽:\n")
                f.write(df.head(8).to_string())
        except Exception as e:
            self._log(f"寫 preview 失敗 {filename}: {e}")

    def parse_file(self, path: str):
        fn = os.path.basename(path)
        if not os.path.isfile(path):
            self._log(f"{fn} 不存在")
            return []

        size = os.path.getsize(path)
        if size < 8000:  # 8KB 以下特別提示
            self._log(f"{fn} 檔案很小({size} bytes)，可能內容極少或為模板")

        info = unified_read(path)
        df = info.get("df")
        mode = info.get("mode")
        error = info.get("error")

        if df is None or df.empty:
            self._log(f"{fn} 讀取/內容為空 mode={mode} error={error}")
            self._write_preview(fn, df, reason=f"empty mode={mode} error={error}", records_len=0)
            return []

        df.columns = df.columns.astype(str).str.strip()
        cols = list(df.columns)

        is_payout = detect_payout_template(cols, fn)

        order_no_col   = _find_first_col(cols, ORDER_NO_CANDIDATES) or _fuzzy_order_col(cols)
        apply_time_col = _find_first_col(cols, APPLY_TIME_CANDIDATES)
        finish_time_col= _find_first_col(cols, FINISH_TIME_CANDIDATES)

        raw_rows = []
        for _, row in df.iterrows():
            if not _is_effective_row(row):
                continue

            # 金額抽取
            in_val = out_val = None
            if is_payout:
                for c in OUT_AMOUNT_CANDIDATES:
                    if c in cols:
                        v = _normalize_amount(row.get(c))
                        if v and v > 0:
                            out_val = v; break
            else:
                for c in IN_AMOUNT_CANDIDATES:
                    if c in cols:
                        v = _normalize_amount(row.get(c))
                        if v and v > 0:
                            in_val = v; break
                for c in OUT_AMOUNT_CANDIDATES:
                    if c in cols:
                        v = _normalize_amount(row.get(c))
                        if v and v > 0:
                            out_val = v; break

            # 若候選欄位都沒值，使用泛用掃描
            if not in_val and not out_val:
                generic_amt = _extract_amount_generic(row)
                if generic_amt:
                    # 藉由 is_payout 推方向，否則以 in
                    if is_payout:
                        out_val = generic_amt
                    else:
                        in_val = generic_amt

            direction = None
            raw_amount = None
            if is_payout:
                if out_val:
                    direction = "out"
                    raw_amount = out_val
                else:
                    continue
            else:
                if in_val and (not out_val or in_val >= out_val):
                    direction = "in"; raw_amount = in_val
                elif out_val and (not in_val or out_val >= in_val):
                    direction = "out"; raw_amount = out_val
                elif in_val:
                    direction = "in"; raw_amount = in_val
                elif out_val:
                    direction = "out"; raw_amount = out_val
                else:
                    # 真的找不到任何金額
                    continue

            amount_int = int(abs(raw_amount))
            amount_str = f"{amount_int:,}"

            # 客戶名稱
            customer_name = ""
            if direction == "in":
                for cnd in IN_PLAYER_NAME_CANDS:
                    if cnd in cols:
                        v = row.get(cnd)
                        if v is not None and str(v).strip():
                            customer_name = str(v).strip(); break
            else:
                for cnd in OUT_RECIPIENT_CANDS:
                    if cnd in cols:
                        v = row.get(cnd)
                        if v is not None and str(v).strip():
                            customer_name = str(v).strip(); break
            if not customer_name:
                customer_name = "買家" if direction == "in" else "收款人"

            # 備註
            note_parts = []
            for mc in MEMO_CANDIDATES:
                if mc in cols:
                    v = row.get(mc)
                    if v is not None and str(v).strip():
                        note_parts.append(f"{mc}:{str(v).strip()}")
            note = " | ".join(note_parts)

            # 單號
            order_no_val = ""
            if order_no_col:
                ov = row.get(order_no_col)
                if ov is not None and str(ov).strip():
                    order_no_val = str(ov).strip()
            if not order_no_val:
                for mc in MEMO_CANDIDATES:
                    if mc in cols:
                        cand = _extract_order_from_text(row.get(mc))
                        if cand:
                            order_no_val = cand; break
            if not order_no_val:
                for c in cols:
                    cl = c.lower()
                    if any(k in cl for k in ["流水","序號","單號","票據","voucher","ref","reference"]):
                        v = row.get(c)
                        if v is not None and str(v).strip():
                            order_no_val = str(v).strip()
                            break

            apply_time_val = ""
            if apply_time_col:
                av = row.get(apply_time_col)
                if av is not None and str(av).strip():
                    apply_time_val = str(av).strip()

            finish_time_val = ""
            if finish_time_col:
                fv = row.get(finish_time_col)
                if fv is not None and str(fv).strip():
                    finish_time_val = str(fv).strip()

            raw_rows.append({
                "direction": direction,
                "amount_int": amount_int,
                "amount": amount_str,
                "customer_name": customer_name,
                "nickname": customer_name,
                "apply_time": apply_time_val,
                "finish_time": finish_time_val,
                "note": note,
                "order_no": order_no_val,
                "_read_mode": mode,
                "source_file": fn
            })

        # 合併相同訂單或 fallback
        merged = {}
        for r in raw_rows:
            if r["order_no"] and not r["order_no"].startswith("FALLBACK_"):
                key = ("ORDER", r["order_no"], r["direction"])
            else:
                base_time = r["apply_time"] or r["finish_time"] or ""
                date_part = base_time[:10]
                key = ("NOORDER", r["customer_name"], r["amount_int"], date_part, r["direction"])
            if key not in merged:
                merged[key] = r.copy()
            else:
                g = merged[key]
                times = [t for t in [g["apply_time"], g["finish_time"], r["apply_time"], r["finish_time"]] if t]
                if times:
                    st = sorted(times)
                    g["apply_time"] = st[0]
                    g["finish_time"] = st[-1]
                if r["note"] and r["note"] not in g["note"]:
                    g["note"] = g["note"] + " || " + r["note"] if g["note"] else r["note"]
                merged[key] = g

        recs = []
        for g in merged.values():
            order_no_final = g["order_no"] or _fallback_order_no(g["apply_time"], g["finish_time"], g["customer_name"], g["amount_int"])
            recs.append({
                "direction": g["direction"],
                "raw_amount_number": g["amount_int"],
                "amount": g["amount"],
                "customer_name": g["customer_name"],
                "nickname": g["customer_name"],
                "time": g["apply_time"] or g["finish_time"],
                "apply_time": g["apply_time"],
                "finish_time": g["finish_time"],
                "note": g["note"],
                "order_no": order_no_final,
                "linkage_id": "",
                "_read_mode": g["_read_mode"],
                "source_file": g["source_file"]
            })

        self._log(f"{fn} 解析完成: 原始列={len(df)} 有效行={len(raw_rows)} 合併後筆數={len(recs)} mode={mode} error={error or '-'}")
        self._write_preview(fn, df, reason=f"parsed mode={mode} error={error or '-'}", records_len=len(recs))

        if DEBUG_ORDER_NO:
            print("[MERGED]", len(recs))
            for i, r in enumerate(recs[:5]):
                print(" sample", i, r["direction"], r["order_no"], r["customer_name"], r["amount"], r["apply_time"], r["finish_time"])

        return recs
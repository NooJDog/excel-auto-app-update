import os
import pandas as pd
import datetime

# -------------------------------
# 安全讀取：支援 .csv / .xlsx / .xls
# .xls -> engine='xlrd'
# 若 .xls 讀失敗 → 檢查是否為偽裝 CSV/HTML → fallback
# -------------------------------

def safe_read(path, header=None):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        try:
            return pd.read_csv(path, encoding="utf-8")
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="utf-8-sig")
    if ext == ".xlsx":
        return pd.read_excel(path, header=header, engine="openpyxl")
    if ext == ".xls":
        try:
            return pd.read_excel(path, header=header, engine="xlrd")
        except Exception:
            # 檢查是否 CSV/HTML 偽裝
            with open(path, "rb") as f:
                head = f.read(4096)
            txt = head.decode(errors="ignore").lower()
            if "<table" in txt or "<html" in txt:
                tables = pd.read_html(path)
                if tables:
                    return tables[0]
            if txt.count(",") >= 3:
                try:
                    return pd.read_csv(path, encoding="utf-8")
                except UnicodeDecodeError:
                    return pd.read_csv(path, encoding="utf-8-sig")
            # 最後嘗試不指定 engine（可能會再失敗）
            return pd.read_excel(path, header=header)
    # 其它副檔名
    try:
        return pd.read_excel(path, header=header, engine="openpyxl")
    except Exception:
        return pd.read_csv(path, encoding="utf-8")

def find_header_row_xlsx(path, keyword_list=("收入金額", "收入", "入帳")):
    ext = os.path.splitext(path)[1].lower()
    if ext not in (".xlsx", ".xls"):
        return None
    try:
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        temp_df = pd.read_excel(path, nrows=30, header=None, engine=engine)
    except Exception:
        try:
            temp_df = pd.read_excel(path, nrows=30, header=None)
        except Exception:
            return None
    for i, row in temp_df.iterrows():
        vals = [str(v) if not pd.isna(v) else "" for v in row.values]
        joined = " ".join(vals)
        for kw in keyword_list:
            if kw in joined:
                return i
    return None

def auto_map(cols):
    cols_norm = [c.strip() for c in cols]
    def f(cands):
        for cand in cands:
            for c in cols_norm:
                if cand.lower() in c.lower():
                    return c
        return None
    return {
        "資料內容": f(["資料內容","名稱","客戶","姓名","name"]),
        "異動日":   f(["異動日","日期","date","交易日","申請時間"]),
        "交易時間": f(["交易時間","時間","time","完成時間"]),
        "收入金額": f(["收入金額","收入","credit","入帳","實付","金額","交易金額"]),
        "摘要":     f(["摘要","備註","remark","description","附言"]),
        "票據號碼": f(["票據號碼","票號","票據","voucher","transaction_no","流水號"]),
    }

def normalize_and_extract(df, mapping):
    df.columns = df.columns.astype(str).str.strip()
    for k,v in mapping.items():
        if v and v not in df.columns:
            df[v] = ""
    income_col = mapping.get("收入金額")
    if not income_col:
        return pd.DataFrame()
    s = (df[income_col].astype(str)
         .str.replace(",", "")
         .str.replace("$", "")
         .str.replace("NT$", "")
         .str.strip())
    nums = pd.to_numeric(s, errors="coerce").fillna(0)
    df["_收入數字"] = nums
    df_income = df[df["_收入數字"] > 0].copy()
    if df_income.empty:
        return pd.DataFrame()

    date_col = mapping.get("異動日")
    time_col = mapping.get("交易時間")
    if date_col:
        try:
            dates = pd.to_datetime(df_income[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        except Exception:
            dates = df_income[date_col].astype(str)
    else:
        dates = pd.Series([""] * len(df_income))
    if time_col and time_col in df_income.columns:
        times = df_income[time_col].astype(str).fillna("")
        order_dt = (dates + " " + times).str.strip()
    else:
        order_dt = dates

    out = pd.DataFrame({
        "billing_name": df_income.get(mapping.get("資料內容"), "").astype(str),
        "order_date": order_dt,
        "total": df_income["_收入數字"],
        "payment_method": "bank_transfer",
        "status": "completed",
        "note": df_income.get(mapping.get("摘要"), "").astype(str) + " / " + df_income.get(mapping.get("票據號碼"), "").astype(str)
    })
    return out

def format_single_file_to_woocommerce(path, output_dir):
    if not os.path.isfile(path):
        return False, f"File not found: {path}", None
    ext = os.path.splitext(path)[1].lower()
    header_row = find_header_row_xlsx(path) if ext in (".xlsx", ".xls") else None
    try:
        df = safe_read(path, header=header_row)
    except Exception as e:
        return False, f"Failed to read {path}: {e}", None
    cols = list(df.columns.astype(str))
    mapping = auto_map(cols)
    if not mapping.get("收入金額"):
        for c in cols:
            if any(k in c.lower() for k in ["amount","total","金額","實付","交易金額"]):
                mapping["收入金額"] = c
                break
    out_df = normalize_and_extract(df, mapping)
    if out_df.empty:
        return False, "No income records found (>0) after parsing", None
    os.makedirs(output_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(output_dir, f"woocommerce_ready_{stamp}.xlsx")
    try:
        out_df.to_excel(out_path, index=False)
        return True, "OK", out_path
    except Exception as e:
        return False, f"Failed to write: {e}", None

def format_files_in_list(file_list, output_dir):
    merged=[]
    processed=0
    skipped=0
    failed=[]
    for f in file_list:
        ok,msg,outp=format_single_file_to_woocommerce(f,output_dir)
        if ok:
            processed+=1
            try:
                df=pd.read_excel(outp, engine="openpyxl")
                merged.append(df)
                try: os.remove(outp)
                except Exception: pass
            except Exception as e:
                failed.append((f, f"Read temp failed:{e}"))
        else:
            if "No income records" in msg:
                skipped+=1
            else:
                failed.append((f,msg))
    if not merged:
        return {"processed":processed,"skipped":skipped,"failed":failed,"output_path":None}
    final=pd.concat(merged, ignore_index=True)
    os.makedirs(output_dir, exist_ok=True)
    stamp=datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    final_path=os.path.join(output_dir,f"woocommerce_ready_{stamp}.xlsx")
    final.to_excel(final_path, index=False)
    return {"processed":processed,"skipped":skipped,"failed":failed,"output_path":final_path}
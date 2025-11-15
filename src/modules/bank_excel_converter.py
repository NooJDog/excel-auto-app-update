import os
import csv
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd

TARGET_COLUMNS = [
    "order_number","order_date","paid_date","status","shipping_total","shipping_tax_total",
    "fee_total","fee_tax_total","tax_total","cart_discount","order_discount","discount_total",
    "order_total","order_subtotal","order_currency","payment_method","payment_method_title",
    "transaction_id","customer_ip_address","customer_user_agent","shipping_method","customer_id",
    "customer_user","customer_email","billing_first_name","billing_last_name","billing_company",
    "billing_email","billing_phone","billing_address_1","billing_address_2","billing_postcode",
    "billing_city","billing_state","billing_country","shipping_first_name","shipping_last_name",
    "shipping_company","shipping_address_1","shipping_address_2","shipping_postcode","shipping_city",
    "shipping_state","shipping_country","customer_note","wt_import_key","shipping_items","tax_items",
    "coupon_items","order_notes","line_item_1","meta:is_vat_exempt","meta:_new_order_email_sent"
]

FIELD_CANDIDATES_IN = {
    "amount": ["實付","金額","交易金額","欲付","amount","total","代收金額","收入"],
    "apply_time": ["申請時間","日期","交易日期","date","建立時間"],
    "paid_time": ["完成時間","付款時間","paid","paid_date","入帳時間"],
    "reference": ["流水號","客單號","交易編號","參考編號","訂單號","訂單編號"],
    "name": ["玩家名","商戶昵稱","對方","名稱","客戶名稱"],
    "memo": ["附言","備註","摘要","description","說明"],
    "status": ["訂單狀態","狀態","status"]
}
FIELD_CANDIDATES_OUT = {
    "amount": ["交易金額","付款金額","金額","amount","total","代付金額","出帳金額","支出金額"],
    "apply_time": ["申請時間","日期","date","建立時間"],
    "paid_time": ["完成時間","付款時間","paid","paid_date"],
    "reference": ["訂單號","商戶單號","交易編號","參考編號","流水號"],
    "name": ["收款人","商戶昵稱","玩家名","名稱","收款方","戶名"],
    "memo": ["收款銀行","收款卡號","附言","備註","摘要","description"],
    "status": ["訂單狀態","狀態","status"]
}

SHOW_OUT_FEE_FORMULA = False

PRODUCT_DISPLAY = {
    "game_currency": "遊戲幣",
    "game_item": "遊戲寶物",
    "used_goods": "二手商品"
}

def detect_encoding_for_csv(path: str) -> Optional[str]:
    candidates = ["utf-8","utf-8-sig","cp950","big5"]
    for enc in candidates:
        try:
            with open(path,"r",encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return None

def read_legacy_xls(path: str):
    try:
        import xlrd
    except ImportError:
        return None, "xlrd_not_installed"
    try:
        wb = xlrd.open_workbook(path)
    except Exception as e:
        return None, f"xlrd_open_fail:{e}"
    sheets = wb.sheets()
    if not sheets:
        return None, "no_sheets"
    sh = sheets[0]
    rows = []
    for r in range(sh.nrows):
        rows.append(sh.row_values(r))
    if not rows:
        return None, "empty_sheet"
    header = rows[0]
    non_empty = sum(1 for v in header if str(v).strip() not in ("","None","nan"))
    if non_empty < 2:
        cols = [f"col_{i}" for i in range(len(header))]
        data_rows = rows
    else:
        cols = [str(v).strip() if str(v).strip() else f"col_{i}" for i,v in enumerate(header)]
        data_rows = rows[1:]
    import pandas as pd
    df = pd.DataFrame(data_rows, columns=cols)
    return df, "xls_manual"

def safe_read(path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xls":
        df, mode = read_legacy_xls(path)
        if df is not None:
            return df, mode, ""
        try:
            df2 = pd.read_excel(path)
            return df2, "xls_auto", ""
        except Exception as e:
            return None, "xls_fail", str(e)

    if ext in (".xlsx",".xlsm"):
        try:
            df = pd.read_excel(path, engine="openpyxl")
            return df, "xlsx", ""
        except Exception as e:
            return None, "xlsx_fail", str(e)

    if ext == ".csv":
        enc = detect_encoding_for_csv(path) or "utf-8"
        try:
            df = pd.read_csv(path, encoding=enc)
            return df, f"csv({enc})", ""
        except Exception as e:
            return None, "csv_fail", f"encoding={enc}; {e}"

    try:
        df = pd.read_excel(path, engine="openpyxl")
        return df, "excel_generic", ""
    except Exception as e1:
        try:
            enc = detect_encoding_for_csv(path) or "utf-8"
            df = pd.read_csv(path, encoding=enc)
            return df, f"csv_fallback({enc})", ""
        except Exception as e2:
            return None, "generic_fail", f"{e1}; {e2}"

def _best_match(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    best = None
    best_score = 0
    for cand in candidates:
        import difflib
        matches = difflib.get_close_matches(cand.lower(), list(lower_map.keys()), n=1, cutoff=0.55)
        if matches:
            score = difflib.SequenceMatcher(None, cand.lower(), matches[0]).ratio()
            if score > best_score:
                best = lower_map[matches[0]]; best_score = score
    return best

def detect_mode(df: pd.DataFrame, filename: str) -> str:
    cols = [c.strip() for c in df.columns.astype(str)]
    fnl = filename.lower()
    if any(k in fnl for k in ["出帳","代付","payout","withdraw"]):
        return "out"
    if "實付" in cols and ("玩家名" in cols or "商戶昵稱" in cols):
        return "in"
    if ("交易金額" in cols or "付款金額" in cols) and ("收款人" in cols or "收款銀行" in cols or "收款卡號" in cols):
        return "out"
    return "in"

def build_mapping(df: pd.DataFrame, mode:str)->Dict[str,str]:
    mapping={}
    cands = FIELD_CANDIDATES_IN if mode=="in" else FIELD_CANDIDATES_OUT
    cols=list(df.columns.astype(str))
    for k,arr in cands.items():
        mapping[k]=_best_match(cols,arr)
    return mapping

def parse_dt(val):
    if val is None or str(val).strip()=="":
        return ""
    try:
        dt=pd.to_datetime(str(val))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val).strip()

def normalize_amount(raw) -> float:
    if raw is None: return 0.0
    s = str(raw).strip()
    if not s: return 0.0
    neg=False
    if s.startswith("(") and s.endswith(")"):
        neg=True; s=s[1:-1]
    s=(s.replace(",","").replace("，","").replace("$","")
        .replace("NT$","").replace("元","").replace(" ",""))
    import re
    m=re.search(r"-?\d+(?:\.\d+)?",s)
    if not m: return 0.0
    v=float(m.group(0))
    return -v if neg else v

def process_file(input_path: str, output_csv: str,
                 fee_rate: float = 0.07,
                 bank_prefix: str = "WT",
                 product_type: str = "game_currency") -> Tuple[int, Dict[str,str], str]:
    if not os.path.isfile(input_path):
        raise FileNotFoundError(input_path)

    df, read_mode, err = safe_read(input_path)
    if df is None or df.empty:
        print(f"[BankConv] 讀取失敗/空: file={os.path.basename(input_path)} mode={read_mode} err={err}")
        return 0, {}, read_mode

    df.columns=df.columns.astype(str).str.strip()
    filename=os.path.basename(input_path)
    mode=detect_mode(df,filename)
    mapping=build_mapping(df,mode)
    out_rows=[]
    percent_str=f"{fee_rate*100:.2f}%"
    prod_disp=PRODUCT_DISPLAY.get(product_type,"遊戲幣")

    for idx,row in df.iterrows():
        if row.isna().all():
            continue
        raw_amount=row.get(mapping.get("amount")) if mapping.get("amount") else None
        amt=abs(normalize_amount(raw_amount))
        if amt==0:
            continue

        apply_time=row.get(mapping.get("apply_time")) if mapping.get("apply_time") else ""
        paid_time=row.get(mapping.get("paid_time")) if mapping.get("paid_time") else apply_time
        order_date=parse_dt(apply_time)
        paid_date=parse_dt(paid_time)
        txid=row.get(mapping.get("reference")) if mapping.get("reference") else ""
        name=row.get(mapping.get("name")) if mapping.get("name") else ""
        name=str(name).strip() if name and str(name).strip() not in ("nan","None") else ("買家" if mode=="in" else "賣家")
        memo_src=[]
        if mapping.get("memo"):
            mv=row.get(mapping["memo"])
            if mv is not None and str(mv).strip():
                memo_src.append(str(mv).strip())
        if mapping.get("status"):
            st=row.get(mapping["status"])
            if st is not None and str(st).strip():
                memo_src.append("來源狀態:"+str(st).strip())

        order={c:"" for c in TARGET_COLUMNS}
        now=datetime.now().strftime("%Y%m%d%H%M%S")
        order["order_number"]=f"{bank_prefix}-{now}-{abs(hash(str(idx)))%10000:04d}"
        order["order_date"]=order_date
        order["paid_date"]=paid_date
        order["order_currency"]="TWD"
        order["payment_method"]="bank_transfer"
        order["payment_method_title"]="銀行轉帳"
        order["transaction_id"]=str(txid)
        order["billing_first_name"]=name
        order["shipping_first_name"]=name

        fee_int=int(amt*fee_rate)

        if mode=="in":
            order["order_total"]=str(fee_int)
            order["order_subtotal"]=order["order_total"]
            order["fee_total"]="0"
            order["status"]="completed"
            base_note=f"備註:{fee_int}(平台手續費) = {int(amt)}(交易金額)*{percent_str} (抽 {percent_str}) (商品:{prod_disp})"
            if memo_src: base_note+=" | "+ " ; ".join(memo_src)
            order["customer_note"]=base_note
            order["line_item_1"]=(
                f"name:平台手續費|product_id:30977|quantity:1|"
                f"total:{order['order_total']}|sub_total:{order['order_total']}"
            )
        else:
            order["order_total"]=str(int(amt))
            order["order_subtotal"]=order["order_total"]
            order["fee_total"]=str(fee_int)
            order["status"]="on-hold"
            if SHOW_OUT_FEE_FORMULA:
                base_note=f"備註:{fee_int}(平台手續費) = {int(amt)}(交易金額)*{percent_str} (抽 {percent_str}) (商品:{prod_disp})"
            else:
                base_note=f"備註:出款 {int(amt)} 元 (商品:{prod_disp})"
            if memo_src: base_note+=" | "+ " ; ".join(memo_src)
            order["customer_note"]=base_note
            order["line_item_1"]=(
                f"name:商品交易|product_id:30978|quantity:1|"
                f"total:{order['order_total']}|sub_total:{order['order_total']}"
            )

        for z in ["shipping_total","shipping_tax_total","fee_tax_total","tax_total",
                  "cart_discount","order_discount","discount_total"]:
            order[z]="0"
        order["wt_import_key"]=str(txid)
        order["meta:is_vat_exempt"]="no"
        order["meta:_new_order_email_sent"]="FALSE"
        out_rows.append(order)

    os.makedirs(os.path.dirname(output_csv) or ".",exist_ok=True)
    with open(output_csv,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=TARGET_COLUMNS,extrasaction="ignore")
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"[BankConv] 檔:{filename} 模式:{mode} 讀取:{read_mode} 轉換:{len(out_rows)} 筆 → {output_csv}")
    return len(out_rows), mapping, read_mode

def cli():
    p=argparse.ArgumentParser()
    p.add_argument("input")
    p.add_argument("output",nargs="?")
    p.add_argument("--fee-rate",type=float,default=0.07)
    p.add_argument("--bank-prefix",default="WT")
    p.add_argument("--product-type",default="game_currency",
                   choices=["game_currency","game_item","used_goods"])
    p.add_argument("--show-out-fee",action="store_true")
    args=p.parse_args()
    global SHOW_OUT_FEE_FORMULA
    if args.show_out_fee: SHOW_OUT_FEE_FORMULA=True
    out=args.output or (os.path.splitext(args.input)[0]+"_woo.csv")
    process_file(args.input,out,fee_rate=args.fee_rate,bank_prefix=args.bank_prefix,
                 product_type=args.product_type)

if __name__=="__main__":
    cli()
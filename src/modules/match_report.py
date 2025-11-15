import os, sqlite3, itertools
from modules.excel_export_utils import (
    create_workbook, autofit_columns, style_header,
    color_diff_cell, finalize_sheet, safe_save_wb
)

STATUS_DISPLAY = {
    ('in','matched'):'代收(媒合完成)',
    ('out','matched'):'代付(媒合完成)',
    ('in','partial'):'代收(部分媒合)',
    ('out','partial'):'代付(部分媒合)',
    ('in','pending'):'代收(待媒合)',
    ('out','pending'):'代付(待媒合)',
}

PRODUCT_CHINESE = {
    "game_currency": "遊戲幣",
    "game_item": "遊戲寶物",
    "used_goods": "二手商品"
}

def ensure_columns(conn):
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    cur.execute("PRAGMA table_info(transactions)")
    existing={r[1] for r in cur.fetchall()}
    needed=[
        "apply_time","finish_time","direction","amount","order_no","customer_name",
        "linkage_id","status","remaining_amount","consumed_amount","match_ratio",
        "cumulative_unmatched_at_row","product_type"
    ]
    for col in needed:
        if col not in existing:
            try: cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError: pass
    if "time" in existing and "apply_time" in existing:
        cur.execute("""UPDATE transactions SET apply_time=COALESCE(apply_time,time)
                       WHERE (apply_time IS NULL OR apply_time='') AND time IS NOT NULL""")
    conn.commit()

def _int(v):
    try: return int(float(str(v)))
    except: return 0

def _fee(v, rate): return int(_int(v)*rate)

def _ratio(consumed, original): return f"{consumed/original:.2f}" if original else ""

def _subset_exact(entries, target):
    if len(entries)==0 or len(entries)>24: return None
    for r in range(1,len(entries)+1):
        for combo in itertools.combinations(entries,r):
            if sum(rem for _,rem in combo)==target:
                return [idx for idx,_ in combo]
    return None

def two_pass_match(conn):
    ensure_columns(conn)
    cur=conn.cursor()
    rows=cur.execute("""
        SELECT id,direction,amount,order_no,apply_time,finish_time
        FROM transactions ORDER BY apply_time,id
    """).fetchall()

    cur.execute("""UPDATE transactions SET
        status=NULL, linkage_id='',
        remaining_amount=NULL, consumed_amount=NULL,
        match_ratio=NULL, cumulative_unmatched_at_row=NULL
    """)
    conn.commit()

    in_entries=[]; out_entries=[]
    for rid,direction,amount,order_no,apply_time,finish_time in rows:
        amt=_int(amount)
        if direction=='in':
            in_entries.append({'id':rid,'amount':amt,'remaining':amt,'consumed':0,'order_no':order_no})
        else:
            out_entries.append({'id':rid,'amount':amt,'matched_consumed':0,'order_no':order_no,'status':'pending'})

    for out in out_entries:
        target=out['amount']
        pool=[(idx,e['remaining']) for idx,e in enumerate(in_entries) if e['remaining']>0]
        pool_sum=sum(rem for _,rem in pool)
        if pool_sum < target:
            out['status']='pending'; out['matched_consumed']=0
            continue
        subset=_subset_exact(pool,target)
        if subset:
            consumed_total=0
            for si in subset:
                e=in_entries[si]
                consumed_total+=e['remaining']
                e['remaining']=0
                e['consumed']+=e['remaining']
                e['status']='matched'
                e['linkage']=out['order_no'] or ''
            out['matched_consumed']=consumed_total
            out['status']='matched'
            continue
        running=0; fifo=[]
        for idx,e in enumerate(in_entries):
            if e['remaining']<=0: continue
            if running>=target: break
            take=min(e['remaining'], target-running)
            running+=take
            fifo.append((idx,take))
        if running==target:
            for idx,take in fifo:
                e=in_entries[idx]
                e['remaining']-=take; e['consumed']+=take
                e['status']='matched'; e['linkage']=out['order_no'] or ''
            out['matched_consumed']=target; out['status']='matched'
        else:
            for idx,take in fifo:
                e=in_entries[idx]
                e['remaining']-=take; e['consumed']+=take
                e['status']='partial' if e['remaining']>0 else 'matched'
                e['linkage']=out['order_no'] or ''
            out['matched_consumed']=running; out['status']='partial'

    in_remaining_map={e['id']:e['remaining'] for e in in_entries}
    cumulative_unmatched=0
    snapshot={}
    for rid,direction,amount,order_no,apply_time,finish_time in rows:
        if direction=='in':
            cumulative_unmatched += in_remaining_map.get(rid,0)
        snapshot[rid]=cumulative_unmatched

    for e in in_entries:
        consumed=e['consumed']; remaining=e['remaining']
        status=e.get('status','pending')
        linkage=e.get('linkage','') if consumed>0 else ''
        ratio=_ratio(consumed,e['amount'])
        conn.execute("""UPDATE transactions SET
            status=?, linkage_id=?, consumed_amount=?, match_ratio=?,
            remaining_amount=?, cumulative_unmatched_at_row=?
            WHERE id=?""",(status, linkage, consumed, ratio, remaining, snapshot[e['id']], e['id']))
    for o in out_entries:
        consumed=o['matched_consumed']
        remaining_out=o['amount']-consumed if o['status']=='partial' else 0
        ratio=_ratio(consumed,o['amount'])
        linkage=o['order_no'] if consumed>0 else ''
        conn.execute("""UPDATE transactions SET
            status=?, linkage_id=?, consumed_amount=?, match_ratio=?,
            remaining_amount=?, cumulative_unmatched_at_row=?
            WHERE id=?""",(o['status'], linkage, consumed, ratio, remaining_out, snapshot[o['id']], o['id']))
    conn.commit()

def generate_match_reports(conn, out_dir: str, fee_rate: float = 0.07):
    two_pass_match(conn)
    cur=conn.cursor()
    detail_rows=cur.execute("""
        SELECT linkage_id, order_no, apply_time, finish_time, customer_name,
               product_type,
               CASE WHEN direction='in' THEN amount ELSE 0 END AS in_amount,
               CASE WHEN direction='out' THEN amount ELSE 0 END AS out_amount,
               direction, status,
               remaining_amount, cumulative_unmatched_at_row
        FROM transactions
        ORDER BY apply_time,id
    """).fetchall()

    summary_rows=cur.execute("""
        SELECT COALESCE(linkage_id,'') AS lid,
               SUM(CASE WHEN direction='in' THEN amount ELSE 0 END) AS sum_in,
               SUM(CASE WHEN direction='out' THEN amount ELSE 0 END) AS sum_out,
               SUM(CASE WHEN direction='in' THEN consumed_amount ELSE 0 END) AS consumed_in_total,
               SUM(CASE WHEN direction='out' THEN consumed_amount ELSE 0 END) AS consumed_out_total,
               COUNT(CASE WHEN direction='in' THEN 1 END) AS cnt_in,
               COUNT(CASE WHEN direction='out' THEN 1 END) AS cnt_out
        FROM transactions
        GROUP BY COALESCE(linkage_id,'')
        ORDER BY lid
    """).fetchall()

    os.makedirs(out_dir,exist_ok=True)
    detail_xlsx=os.path.join(out_dir,"媒合報表明細.xlsx")
    summary_xlsx=os.path.join(out_dir,"媒合報表彙總.xlsx")

    wb=create_workbook(); ws=wb.active; ws.title="媒合明細"
    ws.append([
        "關聯單號","單號","寫入時間","媒合時間","客戶名稱","商品類型",
        "代收金額","代付金額",
        "電子發票","平台手續費","電子發票金額",
        "狀態","個別剩餘金額","累積未媒合金額(當時快照)"
    ])
    style_header(ws)
    percent_display = f"{fee_rate*100:.2f}%"
    for (lid,order_no,apply_t,finish_t,name,ptype,in_amt,out_amt,direction,status,remaining,cum_snap) in detail_rows:
        disp=STATUS_DISPLAY.get((direction,status or 'pending'), f"{'代收' if direction=='in' else '代付'}(待媒合)")
        product_cn=PRODUCT_CHINESE.get(ptype,"遊戲幣")
        if direction=='in':
            platform_fee=percent_display
            invoice_amt=_fee(in_amt, fee_rate)
            invoice_flag=""
        else:
            platform_fee="-"
            invoice_amt="-"
            invoice_flag="-"
        ws.append([
            lid or "", order_no or "", apply_t or "", finish_t or "", name or "", product_cn,
            in_amt or 0, out_amt or 0,
            invoice_flag, platform_fee, invoice_amt,
            disp, remaining or 0, cum_snap or 0
        ])
    autofit_columns(ws); finalize_sheet(ws); safe_save_wb(wb, detail_xlsx)

    wb2=create_workbook(); ws2=wb2.active; ws2.title="媒合彙總"
    ws2.append([
        "關聯單號","代收筆數","代付筆數",
        "代收總額","平台手續費(估)","已媒合代收金額","未媒合代收金額",
        "代付總額","已媒合代付金額","差額絕對值","差額方向",
        "媒合效率","完成狀態"
    ])
    style_header(ws2)
    for (lid,sum_in,sum_out,consumed_in,consumed_out,cnt_in,cnt_out) in summary_rows:
        sum_in_i=_int(sum_in); sum_out_i=_int(sum_out)
        consumed_in_i=_int(consumed_in); consumed_out_i=_int(consumed_out)
        unmatched_in=sum_in_i - consumed_in_i
        diff=sum_in_i - sum_out_i; abs_diff=abs(diff)
        efficiency=f"{(consumed_in_i/sum_in_i*100):.1f}%" if sum_in_i else "0%"
        if lid and cnt_in>0 and cnt_out>0 and diff==0 and unmatched_in==0:
            done="完成"; diff_dir="平衡"
        else:
            done="待媒合"
            if diff>0: diff_dir=f"代收多(+{diff})"
            elif diff<0: diff_dir=f"代付多(+{abs_diff})"
            else: diff_dir="無交易"
        ws2.append([
            lid or "", cnt_in or 0, cnt_out or 0,
            sum_in_i, int(sum_in_i*fee_rate), consumed_in_i, unmatched_in,
            sum_out_i, consumed_out_i, abs_diff, diff_dir,
            efficiency, done
        ])
        diff_cell=ws2.cell(ws2.max_row,10)
        color_diff_cell(diff_cell,diff)
    autofit_columns(ws2); finalize_sheet(ws2); safe_save_wb(wb2, summary_xlsx)

    return {"detail_xlsx": detail_xlsx, "summary_xlsx": summary_xlsx}
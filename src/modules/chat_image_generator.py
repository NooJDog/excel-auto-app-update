# -*- coding: utf-8 -*-
"""
聊天圖片生成 v5-cluster-pilmoji
- 逐 Emoji Cluster 渲染（支援 Variation Selector & ZWJ），真正套用：
  EMOJI_BASELINE_SHIFT / EMOJI_SCALE / EMOJI_EXTRA_X_SHIFT
- 保留泡泡/陰影/時間/批次並行邏輯
- 若 Pilmoji 不可用或渲染失敗，emoji fallback 為文字字型
"""

import os, random, datetime, re
from typing import List, Dict, Any, Optional
from PIL import Image, ImageDraw, ImageFont, ImageOps
from product_dialogues import get_templates

# ====== 你的要求的三個參數 ======
EMOJI_BASELINE_SHIFT = 3       # 正值向下
EMOJI_SCALE          = 1.13    # emoji 高度 / 字體大小 比例
EMOJI_EXTRA_X_SHIFT  = 2       # 貼圖後額外 X 位移

# 其他設定
REQUIRED_MAIN_FONT  = r"C:\Users\jiemi\OneDrive\桌面\excel_auto_app\fonts\NotoSansTC-Regular.ttf"
FORCE_PILMOJI       = True
EMOJI_DEBUG         = False

BUBBLE_LEFT  = (255,255,255)
BUBBLE_RIGHT = (90,215,110)
TEXT_COLOR   = (20,20,20)
TIME_COLOR   = (110,110,110)
READ_TEXT    = "已讀"

MSG_FONT_SIZE       = 27
NAME_FONT_SIZE      = 39
BASE_TIME_FONT_SIZE = 12
TIME_FONT_SCALE     = 1.9
TIME_FONT_SIZE      = max(10,int(BASE_TIME_FONT_SIZE * TIME_FONT_SCALE))
READ_FONT_SIZE      = TIME_FONT_SIZE

BUBBLE_PAD_X   = 18
BUBBLE_PAD_Y   = 14
BUBBLE_EXTRA_W = 12
BUBBLE_EXTRA_H = 8
BUBBLE_OVAL_MIN_RADIUS = 14
AVATAR_SIZE = 64
BUBBLE_MAX_WIDTH = 400
GAP_BETWEEN = 40
TEMPLATE_PADDING_TOP = 150
RIGHT_MARGIN = 24
TIME_OFFSET_X = 10
TIME_OFFSET_Y = 0
TIGHT_GAP  = 6
READ_V_GAP = 4
NAME_HEADER_Y = 23
LINE_HEIGHT_EXTRA = 5
FILENAME_USE_TIMESTAMP_SUFFIX = False
AVATAR_OFFSET_X = 18
AVATAR_OFFSET_Y = 2

MAX_BACK_MINUTES = 120
MIN_STEP_MINUTES = 1
PROB_SAME_MINUTE = 0.30
FORCE_SPREAD     = True

print("[ChatImageVersion] v5-cluster-pilmoji loaded")

# ===== 資源載入 =====
try:
    from modules.resources import locate_template, project_root, get_asset_path
    _USE_RESOURCES_MODULE = True
    DEFAULT_TEMPLATE_PATH = str(locate_template())
except Exception:
    _USE_RESOURCES_MODULE = False
    def project_root():
        here = os.path.abspath(os.path.dirname(__file__))
        return os.path.dirname(os.path.dirname(here))
    DEFAULT_TEMPLATE_PATH = os.path.join(project_root(), "assets", "templates", "S_36.png")

# ===== Pilmoji =====
try:
    from pilmoji import Pilmoji
    _PILMOJI_AVAILABLE = True
except Exception as e:
    _PILMOJI_AVAILABLE = False
    if EMOJI_DEBUG:
        print("[PilmojiImportFail]", repr(e))

# ===== 字型快取 =====
_FONT_CACHE={}
def _load_font(path: Optional[str], size:int, role:str)->ImageFont.FreeTypeFont:
    key=(path or "_default", size, role)
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    if path and os.path.isfile(path):
        try:
            f=ImageFont.truetype(path,size)
            _FONT_CACHE[key]=f
            return f
        except Exception as e:
            if EMOJI_DEBUG: print(f"[FontLoad] {role} fail {path}: {e}")
    f=ImageFont.load_default()
    _FONT_CACHE[key]=f
    return f

def locate_main_font()->str:
    if REQUIRED_MAIN_FONT and os.path.isfile(REQUIRED_MAIN_FONT):
        return REQUIRED_MAIN_FONT
    candidates=[
        os.path.join(project_root(),"fonts","NotoSansTC-Regular.ttf"),
        os.path.join(os.environ.get("WINDIR","C:\\Windows"),"Fonts","msjh.ttc"),
        os.path.join(os.environ.get("WINDIR","C:\\Windows"),"Fonts","msjh.ttf"),
    ]
    for c in candidates:
        if os.path.isfile(c): return c
    return ""

# ===== Emoji Cluster Regex (含 VS-16 / ZWJ) =====
EMOJI_CLUSTER_PATTERN = re.compile(
    "("
    "["
    "\U0001F300-\U0001F6FF"
    "\U0001F900-\U0001FAFF"
    "\U0001F1E6-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "\U00002300-\U0000237F"
    "\U00002000-\U000020FF"
    "]"
    "(?:\uFE0F)?"
    "(?:\u200D(?:["
    "\U0001F300-\U0001F6FF"
    "\U0001F900-\U0001FAFF"
    "\U0001F1E6-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "\U00002300-\U0000237F"
    "\U00002000-\U000020FF"
    "](?:\uFE0F)?))*"
    ")"
)

def split_clusters(line:str)->List[tuple]:
    """回傳 [(kind, text)] kind in {'text','emoji'}"""
    parts=[]; last=0
    for m in EMOJI_CLUSTER_PATTERN.finditer(line):
        st,en=m.start(),m.end()
        if st>last:
            parts.append(("text", line[last:st]))
        parts.append(("emoji", line[st:en]))
        last=en
    if last<len(line):
        parts.append(("text", line[last:]))
    return parts

def text_has_emoji(line:str)->bool:
    return any(k=="emoji" for k,_ in split_clusters(line))

# ===== 時間 =====
def _parse_base_datetime(raw:str)->datetime.datetime:
    if not raw: return datetime.datetime.now()
    fmts=["%Y-%m-%d %H:%M:%S","%Y/%m/%d %H:%M:%S","%Y-%m-%d %H:%M","%Y/%m/%d %H:%M",
          "%Y-%m-%d","%Y/%m/%d","%H:%M:%S","%H:%M"]
    for f in fmts:
        try:
            dt=datetime.datetime.strptime(raw,f)
            if f in ("%H:%M:%S","%H:%M"):
                today=datetime.date.today()
                dt=datetime.datetime.combine(today, dt.time())
            return dt
        except: pass
    m=re.search(r"([0-2]?\d):([0-5]\d)", raw or "")
    if m:
        h=int(m.group(1)); mi=int(m.group(2))
        today=datetime.date.today()
        return datetime.datetime(today.year,today.month,today.day,h,mi,0)
    return datetime.datetime.now()

def _format_time_ampm(dt:datetime.datetime)->str:
    h,m=dt.hour,dt.minute
    period="上午" if h<12 else "下午"
    disp=h
    if disp==0: disp=12
    elif disp>12: disp-=12
    return f"{period} {disp:02d}:{m:02d}"

def _generate_time_series(count:int, base_dt:datetime.datetime)->List[datetime.datetime]:
    if count<=0: return []
    if count==1: return [base_dt]
    offsets=[0]*count
    for i in range(count-2,-1,-1):
        if random.random()<PROB_SAME_MINUTE:
            offsets[i]=offsets[i+1]
        else:
            max_extra=MAX_BACK_MINUTES-offsets[i+1]
            if max_extra<MIN_STEP_MINUTES:
                offsets[i]=offsets[i+1]
            else:
                step=random.randint(MIN_STEP_MINUTES,min(15,max_extra))
                offsets[i]=offsets[i+1]+step
    if FORCE_SPREAD and len(set(offsets))==1:
        spread=0
        for i in range(count-2,-1,-1):
            spread+=random.randint(MIN_STEP_MINUTES,3)
            offsets[i]=spread
    times=[base_dt - datetime.timedelta(minutes=o) for o in offsets]
    return sorted(times)

# ===== 換行：以 cluster 為單位 =====
def _measure_segment(draw,font,segment,is_emoji:bool)->int:
    if is_emoji:
        # 預估寬度：字體大小 * EMOJI_SCALE * （約略 0.9~1.05）可再微調
        return int(font.size * EMOJI_SCALE)
    bbox=draw.textbbox((0,0),segment,font=font)
    return bbox[2]-bbox[0]

def _wrap_text_clusters(draw:ImageDraw.ImageDraw,text:str,font:ImageFont.FreeTypeFont,max_width:int)->List[List[tuple]]:
    """
    回傳行列表；每行為 [(kind,segment),...]
    kind: 'text' or 'emoji'
    """
    clusters = split_clusters(text)
    lines=[]
    current=[]
    current_width=0
    for kind, seg in clusters:
        # 分割文本段再細分為“字詞” (僅在 text 中有空白或很長才拆；中文無空白按整段)
        if kind=="text":
            # 若含空白 → 依空白拆詞
            tokens = seg.split(" ") if " " in seg else [seg]
            for tk in tokens:
                if tk=="":
                    continue
                width=_measure_segment(draw,font,tk,False)
                tentative=current_width + (width if current_width==0 else width+1)
                if tentative>max_width and current:
                    lines.append(current)
                    current=[]; current_width=0
                current.append(("text", tk))
                current_width = sum(_measure_segment(draw,font,s, k=="emoji")+(0 if i==0 else 1)
                                    for i,(k,s) in enumerate(current))
        else:  # emoji cluster
            width=_measure_segment(draw,font,seg,True)
            tentative=current_width + (width if current_width==0 else width+1)
            if tentative>max_width and current:
                lines.append(current)
                current=[]; current_width=0
            current.append(("emoji", seg))
            current_width = sum(_measure_segment(draw,font,s, k=="emoji")+(0 if i==0 else 1)
                                for i,(k,s) in enumerate(current))
    if current: lines.append(current)
    return lines

def _get_line_height(draw:ImageDraw.ImageDraw,font:ImageFont.FreeTypeFont)->int:
    bbox=draw.textbbox((0,0),"測",font=font)
    return (bbox[3]-bbox[1]) + LINE_HEIGHT_EXTRA

# ===== Emoji Cluster 渲染 =====
def render_emoji_cluster(img:Image.Image,x:int,y:int,cluster:str,font:ImageFont.FreeTypeFont)->int:
    ascent,_=font.getmetrics()
    baseline_y=y+ascent
    if _PILMOJI_AVAILABLE and FORCE_PILMOJI:
        try:
            box=font.size*3
            tmp=Image.new("RGBA",(box,box),(0,0,0,0))
            with Pilmoji(tmp) as pm:
                pm.text((0,0),cluster,font=font,fill=(0,0,0,255))
            bbox=tmp.getbbox()
            if bbox:
                glyph=tmp.crop(bbox)
                gh,gw=glyph.size[1],glyph.size[0]
                target_h=int(font.size*EMOJI_SCALE)
                ratio=target_h/gh
                target_w=int(gw*ratio)
                glyph=glyph.resize((target_w,target_h),Image.LANCZOS)
                paste_x=x+EMOJI_EXTRA_X_SHIFT
                paste_y=baseline_y - target_h + EMOJI_BASELINE_SHIFT
                img.paste(glyph,(paste_x,paste_y),glyph)
                return target_w + EMOJI_EXTRA_X_SHIFT
        except Exception as e:
            if EMOJI_DEBUG:
                print("[EmojiClusterFail]",cluster,e)
    # fallback regular font
    draw=ImageDraw.Draw(img)
    draw.text((x,y),cluster,font=font,fill=TEXT_COLOR)
    bb=draw.textbbox((0,0),cluster,font=font)
    return bb[2]-bb[0]

def draw_line_clustered(img:Image.Image,x:int,y:int,line_clusters:List[tuple],font:ImageFont.FreeTypeFont,color:tuple):
    draw=ImageDraw.Draw(img)
    cx=x
    for kind,seg in line_clusters:
        if kind=="text":
            draw.text((cx,y),seg,font=font,fill=color)
            bb=draw.textbbox((0,0),seg,font=font)
            cx+=bb[2]-bb[0]
        else:
            w=render_emoji_cluster(img,cx,y,seg,font)
            cx+=w

def _format_placeholders(t:str,data:Dict[str,Any])->str:
    try:
        return t.format(**data)
    except:
        s=t
        for k,v in data.items():
            s=s.replace("{"+k+"}",str(v))
        return s

def _rounded_rect_dynamic(draw:ImageDraw.ImageDraw,box,bubble_h,fill):
    r=max(int(bubble_h/2),BUBBLE_OVAL_MIN_RADIUS)
    try:
        draw.rounded_rectangle(box,radius=r,fill=fill)
    except:
        draw.rectangle(box,fill=fill)

# ===== 頭像處理 =====
_AVATAR_LIST=[]
_AVATAR_CACHE={}
def _init_avatar_list():
    if _AVATAR_LIST or not _USE_RESOURCES_MODULE: return
    try:
        avatar_dir=get_asset_path("avatars")
        if os.path.isdir(avatar_dir):
            for f in os.listdir(avatar_dir):
                if f.lower().endswith((".png",".jpg",".jpeg",".webp",".gif")):
                    _AVATAR_LIST.append(os.path.join(avatar_dir,f))
    except Exception as e:
        print("[ChatGen] 載入頭像失敗", e)

def _make_circle_avatar(path:str)->Optional[Image.Image]:
    try:
        im=Image.open(path).convert("RGBA")
        fitted=ImageOps.fit(im,(AVATAR_SIZE,AVATAR_SIZE),centering=(0.5,0.5),method=Image.LANCZOS)
        mask=Image.new("L",(AVATAR_SIZE,AVATAR_SIZE),0)
        d=ImageDraw.Draw(mask); d.ellipse((0,0,AVATAR_SIZE,AVATAR_SIZE),fill=255)
        fitted.putalpha(mask)
        return fitted
    except Exception:
        return None

def get_avatar_image()->Optional[Image.Image]:
    if not _AVATAR_LIST: return None
    p=random.choice(_AVATAR_LIST)
    if p in _AVATAR_CACHE: return _AVATAR_CACHE[p]
    av=_make_circle_avatar(p)
    if av: _AVATAR_CACHE[p]=av
    return av

# ===== 單張生成 =====
def generate_image_from_record_template(record:Dict[str,Any],out_path:str,preset:Optional[List[Dict[str,str]]]=None,
                                        template_path:Optional[str]=None,avatar_path:Optional[str]=None,
                                        force_template:bool=True)->str:
    if template_path is None: template_path=DEFAULT_TEMPLATE_PATH
    if force_template and not os.path.isfile(template_path):
        raise FileNotFoundError(template_path)

    base=Image.open(template_path).convert("RGBA") if os.path.isfile(template_path) else Image.new("RGBA",(BUBBLE_MAX_WIDTH+260,1400),(240,240,240,255))
    cw,ch=base.size
    img=Image.new("RGBA",(cw,ch),(0,0,0,0))
    img.paste(base,(0,0))
    draw=ImageDraw.Draw(img)

    raw_amount=str(record.get("amount") or record.get("total") or "").replace(",","")
    try: a_num=float(raw_amount) if raw_amount else 0
    except: a_num=0
    amount_str=f"{int(a_num):,}" if a_num else (record.get("amount") or "0")

    direction=(record.get("direction") or "in").lower()
    product_type=record.get("product_type","game_currency")
    nickname=record.get("customer_name") or record.get("nickname") or ("賣家" if direction=="out" else "買家")
    base_time=record.get("apply_time") or record.get("time") or record.get("date") or ""
    base_dt=_parse_base_datetime(base_time)

    item_name=record.get("item_name") or "寶物"
    goods_name=record.get("goods_name") or "二手物品"

    templates=get_templates(product_type,direction)
    preset=random.choice(templates) if preset is None else preset
    times=_generate_time_series(len(preset),base_dt)
    time_strings=[_format_time_ampm(t) for t in times]

    font_path=locate_main_font()
    font_msg=_load_font(font_path, MSG_FONT_SIZE,"msg")
    font_time=_load_font(font_path, TIME_FONT_SIZE,"time")
    font_read=_load_font(font_path, READ_FONT_SIZE,"read")
    font_name=_load_font(font_path, NAME_FONT_SIZE,"name")

    draw.text((108+75,NAME_HEADER_Y),nickname,font=font_name,fill=(0,0,0))

    avatar_img=None
    if avatar_path and os.path.isfile(avatar_path):
        avatar_img=_make_circle_avatar(avatar_path)
    else:
        _init_avatar_list()
        avatar_img=get_avatar_image()

    y=TEMPLATE_PADDING_TOP
    time_items=[]
    read_items=[]

    for i,m in enumerate(preset):
        role=m.get("role","left")
        t_str=time_strings[i]
        data={"buyer":nickname,"amount":amount_str,"time":t_str,"item_name":item_name,"goods_name":goods_name}
        raw_text=_format_placeholders(m.get("text",""),data)

        # 以 cluster wrap
        line_clusters = _wrap_text_clusters(draw, raw_text, font_msg, BUBBLE_MAX_WIDTH)
        lh=_get_line_height(draw,font_msg)
        max_w=0
        for line in line_clusters:
            line_w=sum(_measure_segment(draw,font_msg,seg,(k=='emoji')) for k,seg in line)
            max_w=max(max_w,line_w)
        bw=max_w + BUBBLE_PAD_X*2 + BUBBLE_EXTRA_W
        bh=lh*len(line_clusters) + BUBBLE_PAD_Y*2 + BUBBLE_EXTRA_H

        if role=="left":
            bx=18+AVATAR_SIZE+12; by=y
            if avatar_img: img.paste(avatar_img,(AVATAR_OFFSET_X,by+AVATAR_OFFSET_Y),avatar_img)
            try:
                shadow=Image.new("RGBA",(cw,ch),(0,0,0,0))
                sd=ImageDraw.Draw(shadow)
                sd.rounded_rectangle((bx+3,by+6,bx+bw+6,by+bh+8),
                                     radius=max(int(bh/2),BUBBLE_OVAL_MIN_RADIUS),
                                     fill=(0,0,0,26))
                img=Image.alpha_composite(img,shadow); draw=ImageDraw.Draw(img)
            except: pass
            _rounded_rect_dynamic(draw,(bx,by,bx+bw,by+bh),bh,BUBBLE_LEFT)
            tx=bx+BUBBLE_PAD_X; ty=by+BUBBLE_PAD_Y
            for line in line_clusters:
                draw_line_clustered(img,tx,ty,line,font_msg,TEXT_COLOR)
                ty+=lh
            tb=draw.textbbox((0,0),t_str,font=font_time)
            t_w=tb[2]-tb[0]; t_h=tb[3]-tb[1]
            time_x=bx+bw+TIME_OFFSET_X
            time_y=by+(bh-t_h)/2+TIME_OFFSET_Y
            if time_x+t_w>cw-6:
                time_x=bx+bw-t_w; time_y=by-t_h-6
            time_items.append((time_x,time_y,t_str,"left"))
            y=by+bh+GAP_BETWEEN
        else:
            bx=cw-RIGHT_MARGIN-bw-12; by=y
            try:
                shadow=Image.new("RGBA",(cw,ch),(0,0,0,0))
                sd=ImageDraw.Draw(shadow)
                sd.rounded_rectangle((bx+3,by+6,bx+bw+6,by+bh+8),
                                     radius=max(int(bh/2),BUBBLE_OVAL_MIN_RADIUS),
                                     fill=(0,0,0,26))
                img=Image.alpha_composite(img,shadow); draw=ImageDraw.Draw(img)
            except: pass
            _rounded_rect_dynamic(draw,(bx,by,bx+bw,by+bh),bh,BUBBLE_RIGHT)
            tx=bx+BUBBLE_PAD_X; ty=by+BUBBLE_PAD_Y
            for line in line_clusters:
                draw_line_clustered(img,tx,ty,line,font_msg,TEXT_COLOR)
                ty+=lh
            tb=draw.textbbox((0,0),t_str,font=font_time)
            t_w=tb[2]-tb[0]; t_h=tb[3]-tb[1]
            rb=draw.textbbox((0,0),READ_TEXT,font=font_read)
            r_w=rb[2]-rb[0]; r_h=rb[3]-rb[1]
            block_right=bx-TIGHT_GAP
            read_x=block_right-r_w
            time_x=block_right-t_w
            total_h=r_h+READ_V_GAP+t_h
            mid=by+bh/2
            top_y=mid-total_h/2
            read_y=top_y; time_y=top_y+r_h+READ_V_GAP
            if read_x<6:
                alt_x=time_x+t_w+6
                if alt_x+r_w<=cw-6:
                    read_x=alt_x
                else:
                    read_x=time_x; read_y=time_y+t_h+4
            time_items.append((time_x,time_y,t_str,"right"))
            read_items.append((read_x,read_y,READ_TEXT))
            y=by+bh+GAP_BETWEEN

        if y>ch-180:
            extra=900
            new_h=ch+extra
            new_img=Image.new("RGBA",(cw,new_h),(0,0,0,0))
            new_img.paste(img,(0,0))
            img=new_img; draw=ImageDraw.Draw(img); ch=new_h

    for (tx,ty,text,side) in time_items:
        draw.text((tx,ty),text,font=font_time,fill=TIME_COLOR)
    for (rx,ry,text) in read_items:
        draw.text((rx,ry),text,font=font_read,fill=TIME_COLOR)

    os.makedirs(os.path.dirname(out_path),exist_ok=True)
    img.convert("RGB").save(out_path,quality=96)
    return out_path

# ===== 批次並行 =====
from concurrent.futures import ThreadPoolExecutor, as_completed

def generate_images_from_records(records:List[Dict[str,Any]],
                                 output_dir:str,
                                 prefix:Optional[str]=None,
                                 template_path:Optional[str]=None,
                                 avatar_path:Optional[str]=None,
                                 force_template:bool=True,
                                 max_workers:int=4)->List[str]:
    os.makedirs(output_dir,exist_ok=True)
    if template_path is None:
        template_path=DEFAULT_TEMPLATE_PATH
    timestamp_suffix=datetime.datetime.now().strftime("%Y%m%d%H%M%S") if FILENAME_USE_TIMESTAMP_SUFFIX else ""
    out_paths=[]
    def _fname(rec, idx):
        direction=(rec.get("direction") or "in").lower()
        pfx=prefix or ("入帳" if direction!="out" else "出帳")
        order_no=(rec.get("order_no") or rec.get("order_number") or rec.get("單號") or "").strip()
        if not order_no:
            order_no=(rec.get("customer_name") or rec.get("nickname") or "record").strip()
        safe=order_no.replace("/","_").replace("\\","_").replace(" ","_").replace(":","")
        name=f"{pfx}_{idx:03d}_{safe}"
        if timestamp_suffix: name+=f"_{timestamp_suffix}"
        return os.path.join(output_dir,name+".png")
    def _task(i, rec):
        path=_fname(rec,i)
        try:
            generate_image_from_record_template(rec,path,template_path=template_path,
                                                avatar_path=avatar_path,force_template=force_template)
            return path
        except Exception as e:
            print("[ChatGen] 產生失敗", i, e)
            return None
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures=[pool.submit(_task,i,r) for i,r in enumerate(records, start=1)]
        for fut in as_completed(futures):
            res=fut.result()
            if res: out_paths.append(res)
    return out_paths
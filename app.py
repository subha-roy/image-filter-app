# app.py — single-page, robust, resume-from-logs, fast previews (parallel), overwrite-safe
import io, json, time, hashlib, ssl, collections, re, concurrent.futures
from typing import Dict, Any, Optional, List, Tuple
import requests
from PIL import Image
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ---------- Safe image defaults ----------
Image.MAX_IMAGE_PIXELS = 80_000_000

st.set_page_config(page_title="Image Triplet Filter", layout="wide")

# ---------- Compact CSS + big red Save in middle ----------
st.markdown("""
<style>
.block-container {padding-top: 0.7rem; padding-bottom: 0.4rem; max-width: 1400px;}
section.main > div {padding-top: 0.1rem;}
h1, h2, h3, h4 {margin: 0.2rem 0;}
[data-testid="stMetricValue"] {font-size: 1.25rem;}
.small-text {font-size: 0.9rem; line-height: 1.3rem;}
.caption {font-size: 0.82rem; color: #aaa;}
img {max-height: 500px; object-fit: contain;}
hr {margin: 0.5rem 0;}
/* Force primary buttons to red (used for Save) */
div[data-testid="stButton"] button[k="save_btn"],
div[data-testid="stButton"] button:where(.primary) {
  background-color: #e11d48 !important; border-color: #e11d48 !important;
}
/* Make center Save button span full width of its column */
div[data-testid="stButton"] button[k="save_btn"] { width: 100%; }
</style>
""", unsafe_allow_html=True)

# =========================== Auth ============================
USERS = {
    "Subhadeep": {"password": "Ado1234", "categories": ["demography"]},
    "Gagan":     {"password": "Ado1234", "categories": ["animal"]},
    "Robustness":{"password": "Ado1234", "categories": ["demography", "animal", "objects"]},
}

def do_login_ui():
    st.title("Image Triplet Filter")
    u = st.text_input("Username", value="", key="login_user")
    p = st.text_input("Password", type="password", value="", key="login_pass")
    if st.button("Sign in", type="primary"):
        info = USERS.get(u)
        if info and info["password"] == p:
            st.session_state.user = u
            st.session_state.allowed = info["categories"]
            st.session_state.cat = info["categories"][0]
            st.session_state.idx_initialized_for = None
            st.rerun()
        else:
            st.error("Invalid credentials")

if "user" not in st.session_state:
    do_login_ui()
    st.stop()

# =========================== Drive helpers ===========================
@st.cache_resource
def get_drive():
    sa_raw = st.secrets["gcp"]["service_account"]
    if isinstance(sa_raw, str):
        if '"private_key"' in sa_raw and "\n" in sa_raw and "\\n" not in sa_raw:
            sa_raw = sa_raw.replace("\r\n", "\\n").replace("\n", "\\n")
        sa = json.loads(sa_raw)
    else:
        sa = dict(sa_raw)
    creds = service_account.Credentials.from_service_account_info(
        sa, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

drive = get_drive()

def _retry_sleep(attempt: int):
    time.sleep(min(1.5 * (2 ** attempt), 6.0))

# --- Soft QPS guard to prevent rate spikes across concurrent annotators ---
_last_calls = collections.deque(maxlen=10)
def _qps_guard(max_qps: float = 4.0):
    now = time.time()
    _last_calls.append(now)
    if len(_last_calls) >= 2:
        span = _last_calls[-1] - _last_calls[0]
        if span > 0:
            qps = (len(_last_calls) - 1) / span
            if qps > max_qps:
                time.sleep(min(0.25, (qps / max_qps - 1.0) * 0.12))

# small in-process cache so UI doesn’t die during brief SSL hiccups
_inproc_text_cache: Dict[str, str] = {}

def _download_bytes_with_retry(drive, file_id: str, attempts: int = 6) -> bytes:
    last_err = None
    for i in range(attempts):
        try:
            _qps_guard()
            req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
            buf = io.BytesIO()
            dl = MediaIoBaseDownload(buf, req)
            done = False
            while not done:
                _qps_guard()
                _, done = dl.next_chunk()
            buf.seek(0)
            return buf.read()
        except (HttpError, ssl.SSLError, ConnectionError, requests.RequestException) as e:
            last_err = e
            _retry_sleep(i)
    raise last_err

def read_text_from_drive(drive, file_id: str) -> str:
    try:
        data = _download_bytes_with_retry(drive, file_id)
        text = data.decode("utf-8", errors="ignore")
        _inproc_text_cache[file_id] = text
        return text
    except Exception:
        cached = _inproc_text_cache.get(file_id)
        if cached is not None:
            return cached
        raise

def write_text_to_drive(drive, file_id: str, text: str):
    media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")),
                              mimetype="text/plain", resumable=False)
    _qps_guard()
    drive.files().update(fileId=file_id, media_body=media,
                         supportsAllDrives=True).execute()
    _inproc_text_cache[file_id] = text

def append_lines_to_drive_text(drive, file_id: str, new_lines: List[str], retries: int = 3):
    for attempt in range(retries):
        try:
            prev = read_text_from_drive(drive, file_id)
            updated = prev + "".join(new_lines)
            write_text_to_drive(drive, file_id, updated)
            return
        except Exception:
            _retry_sleep(attempt)
    # final attempt without merge
    prev = _inproc_text_cache.get(file_id, "")
    updated = prev + "".join(new_lines)
    write_text_to_drive(drive, file_id, updated)

# --- Folder index cache: maps filename -> fileId; 1h TTL (huge QPS win) ---
@st.cache_data(show_spinner=False, ttl=3600, max_entries=64)
def list_folder_index(folder_id: str) -> dict[str, str]:
    drv = get_drive()
    page_token = None
    out = {}
    while True:
        _qps_guard()
        resp = drv.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id,name,mimeType,shortcutDetails)",
            pageSize=1000, pageToken=page_token,
            supportsAllDrives=True, includeItemsFromAllDrives=True, corpora="allDrives"
        ).execute()
        for f in resp.get("files", []):
            out[f["name"]] = f["id"]
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out

def find_file_id_in_folder(drive, folder_id: str, filename: str) -> Optional[str]:
    if not filename:
        return None
    idx = list_folder_index(folder_id)  # cached for 1 hour
    return idx.get(filename)

def delete_file_by_id(drive, file_id: Optional[str]):
    if not file_id: return
    try:
        _qps_guard()
        drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except HttpError:
        pass  # already gone

def create_shortcut_to_file(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
    meta = {
        "name": new_name,
        "mimeType": "application/vnd.google-apps.shortcut",
        "parents": [dest_folder_id],
        "shortcutDetails": {"targetId": src_file_id},
    }
    _qps_guard()
    res = drive.files().create(body=meta, fields="id,name",
                               supportsAllDrives=True).execute()
    return res["id"]

# ================== Fast thumbnails / previews ===================
# pooled HTTP session for thumbnails (keep-alive)
_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=2)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# in-session LRU for previews (distinct from st.cache_data)
if "_lru_previews" not in st.session_state:
    st.session_state._lru_previews = collections.OrderedDict()  # key=(file_id,max_side) -> bytes
_LRU_CAP = 400

def _lru_put(key, val):
    lru = st.session_state._lru_previews
    if key in lru:
        lru.move_to_end(key)
    lru[key] = val
    while len(lru) > _LRU_CAP:
        lru.popitem(last=False)

def _lru_get(key):
    lru = st.session_state._lru_previews
    if key in lru:
        lru.move_to_end(key)
        return lru[key]
    return None

def _upgrade_thumb_url(url: str, max_side: int) -> str:
    if not url:
        return url
    url = re.sub(r"(=s)\d{2,4}\b", rf"\g<1>{max_side}", url)
    url = re.sub(r"([&?])s=\d{2,4}\b", rf"\g<1>s={max_side}", url)
    return url

@st.cache_data(show_spinner=False, max_entries=1024, ttl=5400)
def drive_thumbnail_bytes(file_id: str, max_side: int = 680) -> Optional[bytes]:
    drv = get_drive()
    try:
        _qps_guard()
        meta = drv.files().get(fileId=file_id, fields="thumbnailLink", supportsAllDrives=True).execute()
        url = meta.get("thumbnailLink")
        if not url:
            return None
        url = _upgrade_thumb_url(url, max_side)
        r = _session.get(url, timeout=6)
        if r.ok:
            return r.content
    except Exception:
        pass
    return None

def preview_bytes(file_id: str, max_side: int = 680) -> bytes:
    key = (file_id, max_side)
    cached = _lru_get(key)
    if cached is not None:
        return cached
    tb = drive_thumbnail_bytes(file_id, max_side=max_side)
    src = tb
    if src is None:
        raw = _download_bytes_with_retry(get_drive(), file_id)
        with Image.open(io.BytesIO(raw)) as im:
            im = im.convert("RGB")
            im.thumbnail((max_side, max_side))
            out = io.BytesIO()
            im.save(out, format="JPEG", quality=85, optimize=True)
            src = out.getvalue()
    _lru_put(key, src)
    return src

@st.cache_data(show_spinner=False, max_entries=128, ttl=1800)
def original_bytes(file_id: str) -> bytes:
    return _download_bytes_with_retry(get_drive(), file_id)

def fetch_two_previews_parallel(h_id: Optional[str], a_id: Optional[str], max_side: int = 680) -> tuple[Optional[bytes], Optional[bytes]]:
    res_h = res_a = None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            futs = []
            if h_id: futs.append(("h", ex.submit(preview_bytes, h_id, max_side)))
            if a_id: futs.append(("a", ex.submit(preview_bytes, a_id, max_side)))
            for tag, fut in futs:
                try:
                    data = fut.result(timeout=12)
                except Exception:
                    data = None
                if tag == "h": res_h = data
                else:          res_a = data
    except Exception:
        pass
    return res_h, res_a

def render_preview_bytes(img_bytes: Optional[bytes], caption: str):
    if img_bytes is None:
        st.error(f"Missing image: {caption}")
        return
    try:
        st.image(img_bytes, caption=caption, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to render {caption}: {e}")

# =========================== Category config ===========================
CAT = {
    "demography": {
        "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["demography_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["demography_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["demography_adv_filtered_log_id"],
        "hypo_prefix": "dem_h", "adv_prefix":  "dem_ah",
    },
    "animal": {
        "jsonl_id": st.secrets["gcp"]["animal_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["animal_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["animal_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["animal_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["animal_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["animal_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["animal_adv_filtered_log_id"],
        "hypo_prefix": "ani_h", "adv_prefix":  "ani_ah",
    },
    "objects": {
        "jsonl_id": st.secrets["gcp"]["objects_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["objects_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["objects_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["objects_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["objects_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["objects_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["objects_adv_filtered_log_id"],
        "hypo_prefix": "obj_h", "adv_prefix":  "obj_ah",
    },
}

# ===================== Readers / progress from LOGS ======================
def canonical_user(name: str) -> str:
    return (name or "").strip().lower()

@st.cache_data(show_spinner=False)
def load_meta(jsonl_id: str) -> List[Dict[str, Any]]:
    try:
        _qps_guard()
        drive.files().get(fileId=jsonl_id, fields="id",
                          supportsAllDrives=True).execute()
    except HttpError as e:
        st.error(f"Cannot access JSONL file: {e}"); st.stop()
    raw = read_text_from_drive(drive, jsonl_id)
    out: List[Dict[str, Any]] = []
    for ln in raw.splitlines():
        ln = ln.strip()
        if not ln: continue
        try: out.append(json.loads(ln))
        except Exception: pass
    return out

def latest_rows(jsonl_text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ln in jsonl_text.splitlines():
        ln = ln.strip()
        if not ln: continue
        try: out.append(json.loads(ln))
        except Exception: pass
    return out

@st.cache_data(show_spinner=False)
def load_latest_map_for_annotator(log_file_id: str, who: str) -> Dict[str, Dict]:
    rows = latest_rows(read_text_from_drive(drive, log_file_id))
    target = canonical_user(who)
    m: Dict[str, Dict] = {}
    for r in rows:
        pk = r.get("pair_key") or f"{r.get('hypo_id','')}|{r.get('adversarial_id','')}"
        r["pair_key"] = pk
        ann = canonical_user(r.get("annotator") or r.get("_annotator_canon") or "")
        if not ann:
            ann = target; r["annotator"] = who
        if ann == target:
            m[pk] = r  # last wins
    return m

def build_completion_sets(cat_cfg: dict, who: str) -> Tuple[set, Dict[str, Dict], Dict[str, Dict]]:
    log_h_map = load_latest_map_for_annotator(cat_cfg["log_hypo"], who)
    log_a_map = load_latest_map_for_annotator(cat_cfg["log_adv"],  who)
    completed = set()
    keys = set(log_h_map.keys()) | set(log_a_map.keys())
    for pk in keys:
        s_h = (log_h_map.get(pk, {}).get("status") or "").strip()
        s_a = (log_a_map.get(pk, {}).get("status") or "").strip()
        if s_h and s_a:
            completed.add(pk)
    return completed, log_h_map, log_a_map

def pk_of(e: Dict[str, Any]) -> str:
    return f"{e.get('hypo_id','')}|{e.get('adversarial_id','')}"

def first_undecided_index_for(meta: List[Dict[str, Any]], completed_set: set) -> int:
    for i, e in enumerate(meta):
        if pk_of(e) not in completed_set:
            return i
    return max(0, len(meta) - 1)

def compute_resume_index_from_logs(meta: List[Dict[str, Any]], cfg: dict, who: str) -> int:
    completed_set, _, _ = build_completion_sets(cfg, who)
    for i, e in enumerate(meta):
        if pk_of(e) not in completed_set:
            return i
    return max(0, len(meta) - 1)

# Optional pointer file (hint only — not used for resume logic)
def progress_file_id_for(cat: str, who: str) -> str:
    parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
    fname = f"progress_{cat}_{canonical_user(who)}.txt"
    q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
    _qps_guard()
    resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
                              supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = resp.get("files", [])
    if files: return files[0]["id"]
    media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
    meta = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
    _qps_guard()
    return drive.files().create(body=meta, media_body=media, fields="id",
                                supportsAllDrives=True).execute()["id"]

def load_progress_hint(cat: str, who: str) -> int:
    try:
        fid = progress_file_id_for(cat, who)
        txt = read_text_from_drive(drive, fid).strip()
        return max(0, int(txt or "0"))
    except Exception:
        return 0

def save_progress_hint(cat: str, who: str, idx: int):
    try:
        fid = progress_file_id_for(cat, who)
        write_text_to_drive(drive, fid, str(idx))
    except Exception:
        pass

# ---- Micro buffer for logs (reduces write calls) ----
if "write_buf" not in st.session_state:
    st.session_state.write_buf = {"hypo": [], "adv": [], "last_flush": 0.0}

FLUSH_N = 6
FLUSH_SEC = 20.0

def _flush_logs_if_needed(cfg, force: bool = False):
    now = time.time()
    buf = st.session_state.write_buf
    need = force or (len(buf["hypo"]) >= FLUSH_N) or (now - buf["last_flush"] >= FLUSH_SEC and (buf["hypo"] or buf["adv"]))
    if not need:
        return
    try:
        if buf["hypo"]:
            append_lines_to_drive_text(drive, cfg["log_hypo"], buf["hypo"])
            buf["hypo"].clear()
        if buf["adv"]:
            append_lines_to_drive_text(drive, cfg["log_adv"], buf["adv"])
            buf["adv"].clear()
        buf["last_flush"] = now
    except Exception as e:
        st.session_state.last_save_flash = {"msg": f"Log flush delayed: {e}", "ok": False, "ts": now}

# ========================= UI state =========================
if "cat"  not in st.session_state: st.session_state.cat  = st.session_state.allowed[0]
if "idx"  not in st.session_state: st.session_state.idx  = 0
if "dec"  not in st.session_state: st.session_state.dec  = {}
if "hq"   not in st.session_state: st.session_state.hq   = False
if "saving" not in st.session_state: st.session_state.saving = False
if "last_save_token" not in st.session_state: st.session_state.last_save_token = None
if "idx_initialized_for" not in st.session_state: st.session_state.idx_initialized_for = None

# ========================= MAIN (single page) =========================
st.caption(f"Signed in as **{st.session_state.user}**")
left, right = st.columns([2, 1.2], gap="large")

with right:
    allowed = st.session_state.get("allowed", [])
    cat_pick = st.selectbox("Category", allowed,
                            index=allowed.index(st.session_state.cat) if st.session_state.cat in allowed else 0)
    if cat_pick != st.session_state.cat:
        st.session_state.cat = cat_pick
        st.session_state.dec = {}
        st.session_state.idx_initialized_for = None

    who = st.session_state.user
    cfg  = CAT[st.session_state.cat]
    meta = load_meta(cfg["jsonl_id"])
    completed_set, _, _ = build_completion_sets(cfg, who)

    total_pairs = len(meta)
    completed = sum(1 for e in meta if pk_of(e) in completed_set)
    pending = max(0, total_pairs - completed)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total", total_pairs)
    c2.metric("Completed (you)", completed)
    c3.metric("Pending", pending)

    st.session_state.hq = st.toggle("High quality images", value=st.session_state.hq)

# Auto-jump to first undecided ONCE per category change / cold start — logs are the source of truth
if st.session_state.idx_initialized_for != st.session_state.cat:
    cfg_init  = CAT[st.session_state.cat]
    meta_init = load_meta(cfg_init["jsonl_id"])
    st.session_state.idx = compute_resume_index_from_logs(meta_init, cfg_init, st.session_state.user)
    st.session_state.idx_initialized_for = st.session_state.cat

# ------------------------------ LEFT work area ------------------------------
with left:
    cfg = CAT[st.session_state.cat]
    meta = load_meta(cfg["jsonl_id"])
    if not meta:
        st.warning("No records."); st.stop()

    completed_set, log_h_map, log_a_map = build_completion_sets(cfg, st.session_state.user)

    i = max(0, min(st.session_state.idx, len(meta)-1))
    entry = meta[i]
    hypo_name = entry.get("hypo_id", "")
    adv_name  = entry.get("adversarial_id", "")
    pk        = f"{hypo_name}|{adv_name}"

    saved_h_row = (log_h_map.get(pk, {}) or {})
    saved_a_row = (log_a_map.get(pk, {}) or {})
    saved_h = (saved_h_row.get("status") or "").strip() or None
    saved_a = (saved_a_row.get("status") or "").strip() or None
    saved_h_copied_id = saved_h_row.get("copied_id")
    saved_a_copied_id = saved_a_row.get("copied_id")

    if pk not in st.session_state.dec:
        st.session_state.dec[pk] = {"hypo": saved_h, "adv": saved_a}

    st.markdown(f"### {entry.get('id','(no id)')} — <code>{pk}</code>", unsafe_allow_html=True)

    # TEXT + collapsible details
    st.markdown(f'**TEXT**: {entry.get("text","")}')
    cexp1, cexp2 = st.columns(2)
    with cexp1:
        with st.expander("HYPOTHESIS (non-prototype) — show/hide", expanded=False):
            st.markdown(f'<div class="small-text">{entry.get("hypothesis","")}</div>', unsafe_allow_html=True)
    with cexp2:
        with st.expander("ADVERSARIAL (prototype) — show/hide", expanded=False):
            st.markdown(f'<div class="small-text">{entry.get("adversarial","")}</div>', unsafe_allow_html=True)

    # Resolve IDs (fast via cached index), fetch previews in parallel, render instantly
    src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name)
    src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)
    prev_h_bytes, prev_a_bytes = fetch_two_previews_parallel(src_h_id, src_a_id, max_side=680)

    imgL, imgR = st.columns(2, gap="large")

    with imgL:
        st.markdown("**Hypothesis (non-proto)**")
        render_preview_bytes(prev_h_bytes, hypo_name)
        if st.session_state.hq and src_h_id:
            try:
                full_h = original_bytes(src_h_id)
                st.image(full_h, caption=hypo_name + " (HQ)", use_container_width=True)
            except Exception:
                pass
        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Accept (hypo)", key=f"acc_h_{pk}"):
                st.session_state.dec[pk]["hypo"] = "accepted"
        with b2:
            if st.button("❌ Reject (hypo)", key=f"rej_h_{pk}"):
                st.session_state.dec[pk]["hypo"] = "rejected"
        cur_h = st.session_state.dec[pk]["hypo"]
        st.markdown(f'<div class="caption">Current: <b>{cur_h if cur_h else "—"}</b> | '
                    f'Saved: <b>{saved_h or "—"}</b></div>', unsafe_allow_html=True)

    with imgR:
        st.markdown("**Adversarial (proto)**")
        render_preview_bytes(prev_a_bytes, adv_name)
        if st.session_state.hq and src_a_id:
            try:
                full_a = original_bytes(src_a_id)
                st.image(full_a, caption=adv_name + " (HQ)", use_container_width=True)
            except Exception:
                pass
        b3, b4 = st.columns(2)
        with b3:
            if st.button("✅ Accept (adv)", key=f"acc_a_{pk}"):
                st.session_state.dec[pk]["adv"] = "accepted"
        with b4:
            if st.button("❌ Reject (adv)", key=f"rej_a_{pk}"):
                st.session_state.dec[pk]["adv"] = "rejected"
        cur_a = st.session_state.dec[pk]["adv"]
        st.markdown(f'<div class="caption">Current: <b>{cur_a if cur_a else "—"}</b> | '
                    f'Saved: <b>{saved_a or "—"}</b></div>', unsafe_allow_html=True)

    st.markdown("<hr/>", unsafe_allow_html=True)

    # ---------- SAVE & NAV (overwrite-safe + cleanup) ----------
    def save_now():
        st.session_state.saving = True

        dec = st.session_state.dec[pk]
        cur_h, cur_a = dec.get("hypo"), dec.get("adv")
        if cur_h not in {"accepted", "rejected"} or cur_a not in {"accepted", "rejected"}:
            st.session_state.saving = False
            st.session_state.last_save_flash = {"msg": "Decide both sides before saving.", "ok": False, "ts": time.time()}
            return

        ts  = int(time.time())
        base = dict(entry)
        base["pair_key"]  = pk
        base["annotator"] = st.session_state.user
        base["_annotator_canon"] = canonical_user(st.session_state.user)

        new_h_status, new_a_status = cur_h, cur_a

        prev_h_copied = saved_h_copied_id
        prev_a_copied = saved_a_copied_id
        new_h_copied  = prev_h_copied
        new_a_copied  = prev_a_copied

        try:
            # HYPOTHESIS shortcuts (flip-safe)
            if saved_h == "accepted" and new_h_status != "accepted":
                delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg["dst_hypo"], hypo_name))
                new_h_copied = None
            if new_h_status == "accepted":
                delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg["dst_hypo"], hypo_name))
                if src_h_id:
                    new_h_copied = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg["dst_hypo"])

            # ADVERSARIAL shortcuts (flip-safe)
            if saved_a == "accepted" and new_a_status != "accepted":
                delete_file_by_id(drive, prev_a_copied or find_file_id_in_folder(drive, cfg["dst_adv"], adv_name))
                new_a_copied = None
            if new_a_status == "accepted":
                delete_file_by_id(drive, prev_a_copied or find_file_id_in_folder(drive, cfg["dst_adv"], adv_name))
                if src_a_id:
                    new_a_copied = create_shortcut_to_file(drive, src_a_id, adv_name, cfg["dst_adv"])
        except HttpError as e:
            st.session_state.saving = False
            st.session_state.last_save_flash = {"msg": f"Drive shortcut update failed: {e}", "ok": False, "ts": time.time()}
            return

        rec_h = dict(base); rec_h.update({"side":"hypothesis", "status": new_h_status, "decided_at": ts})
        if new_h_copied: rec_h["copied_id"] = new_h_copied
        rec_a = dict(base); rec_a.update({"side":"adversarial", "status": new_a_status, "decided_at": ts})
        if new_a_copied: rec_a["copied_id"] = new_a_copied

        token = hashlib.sha1(json.dumps(
            {"pk":pk, "h":rec_h["status"], "a":rec_a["status"], "who":base["_annotator_canon"]}
        ).encode()).hexdigest()
        if st.session_state.last_save_token == token:
            st.session_state.saving = False
            st.session_state.last_save_flash = {"msg": "Already saved this exact decision.", "ok": True, "ts": time.time()}
            return

        # Buffered appends → fewer drive writes
        try:
            st.session_state.write_buf["hypo"].append(json.dumps(rec_h, ensure_ascii=False) + "\n")
            st.session_state.write_buf["adv"].append(json.dumps(rec_a, ensure_ascii=False) + "\n")
            _flush_logs_if_needed(cfg, force=False)
        except Exception as e:
            st.session_state.saving = False
            st.session_state.last_save_flash = {"msg": f"Failed to buffer logs: {e}", "ok": False, "ts": time.time()}
            return

        # Invalidate caches for readers
        try: load_meta.clear()
        except: pass
        try: load_latest_map_for_annotator.clear()
        except: pass

        st.session_state.last_save_token = token
        st.session_state.saving = False

        # Decide next index purely from logs (source of truth), then prefetch next previews
        meta_local = load_meta(cfg["jsonl_id"])
        next_idx = compute_resume_index_from_logs(meta_local, cfg, st.session_state.user)
        st.session_state.idx = next_idx
        save_progress_hint(st.session_state.cat, st.session_state.user, next_idx)

        # Prefetch previews for the next index (and one ahead) to keep navigation instant
        try:
            j = st.session_state.idx
            if 0 <= j < len(meta_local):
                cur_next = meta_local[j]
                n_h = find_file_id_in_folder(drive, cfg["src_hypo"], cur_next.get("hypo_id",""))
                n_a = find_file_id_in_folder(drive, cfg["src_adv"],  cur_next.get("adversarial_id",""))
                if n_h: _ = preview_bytes(n_h, max_side=680)
                if n_a: _ = preview_bytes(n_a, max_side=680)
                if j+1 < len(meta_local):
                    nxt2 = meta_local[j+1]
                    n2_h = find_file_id_in_folder(drive, cfg["src_hypo"], nxt2.get("hypo_id",""))
                    n2_a = find_file_id_in_folder(drive, cfg["src_adv"],  nxt2.get("adversarial_id",""))
                    if n2_h: _ = preview_bytes(n2_h, max_side=680)
                    if n2_a: _ = preview_bytes(n2_a, max_side=680)
        except Exception:
            pass

        st.session_state.last_save_flash = {"msg": "Saved.", "ok": True, "ts": time.time()}

    # ========== NAV row — Prev | BIG RED Save | Next ==========
    navL, navC, navR = st.columns([1, 4, 1])
    with navL:
        if st.button("⏮ Prev", use_container_width=True):
            st.session_state.idx = max(0, i-1)
            save_progress_hint(st.session_state.cat, st.session_state.user, st.session_state.idx)
            _flush_logs_if_needed(cfg, force=False)
            # Prefetch current + next
            try:
                j = st.session_state.idx
                if 0 <= j < len(meta):
                    curp = meta[j]
                    p_h = find_file_id_in_folder(drive, cfg["src_hypo"], curp.get("hypo_id",""))
                    p_a = find_file_id_in_folder(drive, cfg["src_adv"],  curp.get("adversarial_id",""))
                    if p_h: _ = preview_bytes(p_h, max_side=680)
                    if p_a: _ = preview_bytes(p_a, max_side=680)
                    if j+1 < len(meta):
                        nxt = meta[j+1]
                        n_h = find_file_id_in_folder(drive, cfg["src_hypo"], nxt.get("hypo_id",""))
                        n_a = find_file_id_in_folder(drive, cfg["src_adv"],  nxt.get("adversarial_id",""))
                        if n_h: _ = preview_bytes(n_h, max_side=680)
                        if n_a: _ = preview_bytes(n_a, max_side=680)
            except Exception:
                pass
            st.rerun()

    cur = st.session_state.dec.get(pk, {})
    can_save = (cur.get("hypo") in {"accepted", "rejected"}) and (cur.get("adv") in {"accepted", "rejected"})

    with navC:
        st.button("💾 Save", key="save_btn", type="primary",
                  disabled=(st.session_state.saving or not can_save),
                  on_click=save_now, use_container_width=True)

    with navR:
        if st.button("Next ⏭", use_container_width=True):
            st.session_state.idx = min(len(meta)-1, i+1)
            save_progress_hint(st.session_state.cat, st.session_state.user, st.session_state.idx)
            _flush_logs_if_needed(cfg, force=False)
            try:
                j = st.session_state.idx
                if 0 <= j < len(meta):
                    curp = meta[j]
                    p_h = find_file_id_in_folder(drive, cfg["src_hypo"], curp.get("hypo_id",""))
                    p_a = find_file_id_in_folder(drive, cfg["src_adv"],  curp.get("adversarial_id",""))
                    if p_h: _ = preview_bytes(p_h, max_side=680)
                    if p_a: _ = preview_bytes(p_a, max_side=680)
                    if j+1 < len(meta):
                        nxt = meta[j+1]
                        n_h = find_file_id_in_folder(drive, cfg["src_hypo"], nxt.get("hypo_id",""))
                        n_a = find_file_id_in_folder(drive, cfg["src_adv"],  nxt.get("adversarial_id",""))
                        if n_h: _ = preview_bytes(n_h, max_side=680)
                        if n_a: _ = preview_bytes(n_a, max_side=680)
            except Exception:
                pass
            st.rerun()

    # ---- Flash area directly UNDER Prev | Save | Next ----
    flash = st.session_state.get("last_save_flash")
    if flash:
        (st.success if flash.get("ok") else st.error)(flash["msg"])
# # app.py — single-page, retry-hardened, overwrite-safe, compact UI
# import io, json, time, hashlib, ssl
# from typing import Dict, Any, Optional, List, Tuple
# import requests
# from PIL import Image
# import streamlit as st

# # Google Drive API
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# # ---------- Safe image defaults ----------
# Image.MAX_IMAGE_PIXELS = 80_000_000

# st.set_page_config(page_title="Image Triplet Filter", layout="wide")

# # ---------- Compact CSS + big red Save in middle ----------
# st.markdown("""
# <style>
# .block-container {padding-top: 0.7rem; padding-bottom: 0.4rem; max-width: 1400px;}
# section.main > div {padding-top: 0.1rem;}
# h1, h2, h3, h4 {margin: 0.2rem 0;}
# [data-testid="stMetricValue"] {font-size: 1.25rem;}
# .small-text {font-size: 0.9rem; line-height: 1.3rem;}
# .caption {font-size: 0.82rem; color: #aaa;}
# img {max-height: 500px; object-fit: contain;} /* adjust to 460 if you prefer */
# hr {margin: 0.5rem 0;}
# /* Force primary buttons to red (used for Save) */
# div[data-testid="stButton"] button[k="save_btn"], 
# div[data-testid="stButton"] button:where(.primary) {
#   background-color: #e11d48 !important; border-color: #e11d48 !important;
# }
# /* Make center Save button span full width of its column */
# div[data-testid="stButton"] button[k="save_btn"] { width: 100%; }
# </style>
# """, unsafe_allow_html=True)

# # =========================== Auth ============================
# USERS = {
#     "Subhadeep": {"password": "Ado1234", "categories": ["demography"]},
#     "Gagan":     {"password": "Ado1234", "categories": ["animal"]},
#     "Robustness":{"password": "Ado1234", "categories": ["demography", "animal", "objects"]},
# }

# def do_login_ui():
#     st.title("Image Triplet Filter")
#     u = st.text_input("Username", value="", key="login_user")
#     p = st.text_input("Password", type="password", value="", key="login_pass")
#     if st.button("Sign in", type="primary"):
#         info = USERS.get(u)
#         if info and info["password"] == p:
#             st.session_state.user = u
#             st.session_state.allowed = info["categories"]
#             st.session_state.cat = info["categories"][0]
#             st.session_state.idx_initialized_for = None
#             st.rerun()
#         else:
#             st.error("Invalid credentials")

# if "user" not in st.session_state:
#     do_login_ui()
#     st.stop()

# # =========================== Drive helpers ===========================
# @st.cache_resource
# def get_drive():
#     sa_raw = st.secrets["gcp"]["service_account"]
#     if isinstance(sa_raw, str):
#         if '"private_key"' in sa_raw and "\n" in sa_raw and "\\n" not in sa_raw:
#             sa_raw = sa_raw.replace("\r\n", "\\n").replace("\n", "\\n")
#         sa = json.loads(sa_raw)
#     else:
#         sa = dict(sa_raw)
#     creds = service_account.Credentials.from_service_account_info(
#         sa, scopes=["https://www.googleapis.com/auth/drive"]
#     )
#     return build("drive", "v3", credentials=creds)

# drive = get_drive()

# def _retry_sleep(attempt: int):
#     time.sleep(min(1.5 * (2 ** attempt), 6.0))

# # small in-process cache so UI doesn’t die during brief SSL hiccups
# _inproc_text_cache: Dict[str, str] = {}

# def _download_bytes_with_retry(drive, file_id: str, attempts: int = 6) -> bytes:
#     last_err = None
#     for i in range(attempts):
#         try:
#             req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
#             buf = io.BytesIO()
#             dl = MediaIoBaseDownload(buf, req)
#             done = False
#             while not done:
#                 _, done = dl.next_chunk()
#             buf.seek(0)
#             return buf.read()
#         except (HttpError, ssl.SSLError, ConnectionError, requests.RequestException) as e:
#             last_err = e
#             _retry_sleep(i)
#     raise last_err

# def read_text_from_drive(drive, file_id: str) -> str:
#     try:
#         data = _download_bytes_with_retry(drive, file_id)
#         text = data.decode("utf-8", errors="ignore")
#         _inproc_text_cache[file_id] = text
#         return text
#     except Exception:
#         cached = _inproc_text_cache.get(file_id)
#         if cached is not None:
#             st.info("Drive read hiccup — used cached log contents; UI stays responsive.")
#             return cached
#         raise

# def write_text_to_drive(drive, file_id: str, text: str):
#     media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")),
#                               mimetype="text/plain", resumable=False)
#     drive.files().update(fileId=file_id, media_body=media,
#                          supportsAllDrives=True).execute()
#     _inproc_text_cache[file_id] = text

# def append_lines_to_drive_text(drive, file_id: str, new_lines: List[str], retries: int = 3):
#     for attempt in range(retries):
#         try:
#             prev = read_text_from_drive(drive, file_id)
#             updated = prev + "".join(new_lines)
#             write_text_to_drive(drive, file_id, updated)
#             return
#         except Exception:
#             _retry_sleep(attempt)
#     # final attempt without merge
#     prev = _inproc_text_cache.get(file_id, "")
#     updated = prev + "".join(new_lines)
#     write_text_to_drive(drive, file_id, updated)

# def find_file_id_in_folder(drive, folder_id: str, filename: str) -> Optional[str]:
#     if not filename: return None
#     q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
#     resp = drive.files().list(
#         q=q, spaces="drive", fields="files(id,name,mimeType,shortcutDetails)", pageSize=10,
#         supportsAllDrives=True, includeItemsFromAllDrives=True, corpora="allDrives"
#     ).execute()
#     files = resp.get("files", [])
#     return files[0]["id"] if files else None

# def delete_file_by_id(drive, file_id: Optional[str]):
#     if not file_id: return
#     try:
#         drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
#     except HttpError:
#         pass  # already gone

# def create_shortcut_to_file(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
#     meta = {
#         "name": new_name,
#         "mimeType": "application/vnd.google-apps.shortcut",
#         "parents": [dest_folder_id],
#         "shortcutDetails": {"targetId": src_file_id},
#     }
#     res = drive.files().create(body=meta, fields="id,name",
#                                supportsAllDrives=True).execute()
#     return res["id"]

# # ================== Thumbnails / Full-res ===================
# @st.cache_data(show_spinner=False, max_entries=512, ttl=3600)
# def drive_thumbnail_bytes(file_id: str) -> Optional[bytes]:
#     drv = get_drive()
#     try:
#         meta = drv.files().get(fileId=file_id, fields="thumbnailLink",
#                                supportsAllDrives=True).execute()
#         url = meta.get("thumbnailLink")
#         if not url: return None
#         r = requests.get(url, timeout=10)
#         if r.ok: return r.content
#     except Exception:
#         pass
#     return None

# @st.cache_data(show_spinner=False, max_entries=256, ttl=3600)
# def preview_bytes(file_id: str, max_side: int = 680) -> bytes:
#     tb = drive_thumbnail_bytes(file_id)
#     src = tb if tb is not None else _download_bytes_with_retry(get_drive(), file_id)
#     with Image.open(io.BytesIO(src)) as im:
#         im = im.convert("RGB")
#         im.thumbnail((max_side, max_side))
#         out = io.BytesIO()
#         im.save(out, format="JPEG", quality=88, optimize=True)
#         return out.getvalue()

# @st.cache_data(show_spinner=False, max_entries=128, ttl=1800)
# def original_bytes(file_id: str) -> bytes:
#     return _download_bytes_with_retry(get_drive(), file_id)

# def show_image(file_id: Optional[str], caption: str, high_quality: bool):
#     if not file_id:
#         st.error(f"Missing image: {caption}"); return
#     try:
#         data = original_bytes(file_id) if high_quality else preview_bytes(file_id)
#         st.image(data, caption=caption, use_container_width=True)
#     except Exception as e:
#         st.error(f"Failed to render {caption}: {e}")

# # =========================== Category config ===========================
# CAT = {
#     "demography": {
#         "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],
#         "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
#         "src_adv":  st.secrets["gcp"]["demography_adv_folder"],
#         "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],
#         "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],
#         "log_hypo": st.secrets["gcp"]["demography_hypo_filtered_log_id"],
#         "log_adv":  st.secrets["gcp"]["demography_adv_filtered_log_id"],
#         "hypo_prefix": "dem_h", "adv_prefix":  "dem_ah",
#     },
#     "animal": {
#         "jsonl_id": st.secrets["gcp"]["animal_jsonl_id"],
#         "src_hypo": st.secrets["gcp"]["animal_hypo_folder"],
#         "src_adv":  st.secrets["gcp"]["animal_adv_folder"],
#         "dst_hypo": st.secrets["gcp"]["animal_hypo_filtered"],
#         "dst_adv":  st.secrets["gcp"]["animal_adv_filtered"],
#         "log_hypo": st.secrets["gcp"]["animal_hypo_filtered_log_id"],
#         "log_adv":  st.secrets["gcp"]["animal_adv_filtered_log_id"],
#         "hypo_prefix": "ani_h", "adv_prefix":  "ani_ah",
#     },
#     "objects": {
#         "jsonl_id": st.secrets["gcp"]["objects_jsonl_id"],
#         "src_hypo": st.secrets["gcp"]["objects_hypo_folder"],
#         "src_adv":  st.secrets["gcp"]["objects_adv_folder"],
#         "dst_hypo": st.secrets["gcp"]["objects_hypo_filtered"],
#         "dst_adv":  st.secrets["gcp"]["objects_adv_filtered"],
#         "log_hypo": st.secrets["gcp"]["objects_hypo_filtered_log_id"],
#         "log_adv":  st.secrets["gcp"]["objects_adv_filtered_log_id"],
#         "hypo_prefix": "obj_h", "adv_prefix":  "obj_ah",
#     },
# }

# # ===================== Readers / progress from LOGS ======================
# def canonical_user(name: str) -> str:
#     return (name or "").strip().lower()

# @st.cache_data(show_spinner=False)
# def load_meta(jsonl_id: str) -> List[Dict[str, Any]]:
#     try:
#         drive.files().get(fileId=jsonl_id, fields="id",
#                           supportsAllDrives=True).execute()
#     except HttpError as e:
#         st.error(f"Cannot access JSONL file: {e}"); st.stop()
#     raw = read_text_from_drive(drive, jsonl_id)
#     out: List[Dict[str, Any]] = []
#     for ln in raw.splitlines():
#         ln = ln.strip()
#         if not ln: continue
#         try: out.append(json.loads(ln))
#         except Exception: pass
#     return out

# def latest_rows(jsonl_text: str) -> List[Dict[str, Any]]:
#     out: List[Dict[str, Any]] = []
#     for ln in jsonl_text.splitlines():
#         ln = ln.strip()
#         if not ln: continue
#         try: out.append(json.loads(ln))
#         except Exception: pass
#     return out

# @st.cache_data(show_spinner=False)
# def load_latest_map_for_annotator(log_file_id: str, who: str) -> Dict[str, Dict]:
#     rows = latest_rows(read_text_from_drive(drive, log_file_id))
#     target = canonical_user(who)
#     m: Dict[str, Dict] = {}
#     for r in rows:
#         pk = r.get("pair_key") or f"{r.get('hypo_id','')}|{r.get('adversarial_id','')}"
#         r["pair_key"] = pk
#         ann = canonical_user(r.get("annotator") or r.get("_annotator_canon") or "")
#         if not ann:
#             ann = target; r["annotator"] = who
#         if ann == target:
#             m[pk] = r  # last wins
#     return m

# def build_completion_sets(cat_cfg: dict, who: str) -> Tuple[set, Dict[str, Dict], Dict[str, Dict]]:
#     log_h_map = load_latest_map_for_annotator(cat_cfg["log_hypo"], who)
#     log_a_map = load_latest_map_for_annotator(cat_cfg["log_adv"],  who)
#     completed = set()
#     keys = set(log_h_map.keys()) | set(log_a_map.keys())
#     for pk in keys:
#         s_h = (log_h_map.get(pk, {}).get("status") or "").strip()
#         s_a = (log_a_map.get(pk, {}).get("status") or "").strip()
#         if s_h and s_a:
#             completed.add(pk)
#     return completed, log_h_map, log_a_map

# def pk_of(e: Dict[str, Any]) -> str:
#     return f"{e.get('hypo_id','')}|{e.get('adversarial_id','')}"

# def first_undecided_index_for(meta: List[Dict[str, Any]], completed_set: set) -> int:
#     for i, e in enumerate(meta):
#         if pk_of(e) not in completed_set:
#             return i
#     return max(0, len(meta) - 1)

# # Optional pointer file (hint only)
# def progress_file_id_for(cat: str, who: str) -> str:
#     parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
#     fname = f"progress_{cat}_{canonical_user(who)}.txt"
#     q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
#     resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
#                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
#     files = resp.get("files", [])
#     if files: return files[0]["id"]
#     media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
#     meta = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
#     return drive.files().create(body=meta, media_body=media, fields="id",
#                                 supportsAllDrives=True).execute()["id"]

# def load_progress_hint(cat: str, who: str) -> int:
#     try:
#         fid = progress_file_id_for(cat, who)
#         txt = read_text_from_drive(drive, fid).strip()
#         return max(0, int(txt or "0"))
#     except Exception:
#         return 0

# def save_progress_hint(cat: str, who: str, idx: int):
#     try:
#         fid = progress_file_id_for(cat, who)
#         write_text_to_drive(drive, fid, str(idx))
#     except Exception:
#         pass

# # ========================= UI state =========================
# if "cat"  not in st.session_state: st.session_state.cat  = st.session_state.allowed[0]
# if "idx"  not in st.session_state: st.session_state.idx  = 0
# if "dec"  not in st.session_state: st.session_state.dec  = {}
# if "hq"   not in st.session_state: st.session_state.hq   = False
# if "saving" not in st.session_state: st.session_state.saving = False
# if "last_save_token" not in st.session_state: st.session_state.last_save_token = None
# if "idx_initialized_for" not in st.session_state: st.session_state.idx_initialized_for = None

# # ========================= MAIN (single page) =========================
# st.caption(f"Signed in as **{st.session_state.user}**")
# left, right = st.columns([2, 1.2], gap="large")

# with right:
#     allowed = st.session_state.get("allowed", [])
#     cat_pick = st.selectbox("Category", allowed,
#                             index=allowed.index(st.session_state.cat) if st.session_state.cat in allowed else 0)
#     if cat_pick != st.session_state.cat:
#         st.session_state.cat = cat_pick
#         st.session_state.dec = {}
#         st.session_state.idx_initialized_for = None

#     who = st.session_state.user
#     cfg  = CAT[st.session_state.cat]
#     meta = load_meta(cfg["jsonl_id"])
#     completed_set, _, _ = build_completion_sets(cfg, who)

#     total_pairs = len(meta)
#     completed = sum(1 for e in meta if pk_of(e) in completed_set)
#     pending = max(0, total_pairs - completed)

#     c1, c2, c3 = st.columns(3)
#     c1.metric("Total", total_pairs)
#     c2.metric("Completed (you)", completed)
#     c3.metric("Pending", pending)

#     st.session_state.hq = st.toggle("High quality images", value=st.session_state.hq)

# # Auto-jump to first undecided once per category / first load
# if st.session_state.idx_initialized_for != st.session_state.cat:
#     meta_for_init = load_meta(CAT[st.session_state.cat]["jsonl_id"])
#     comp_set_init, _, _ = build_completion_sets(CAT[st.session_state.cat], st.session_state.user)
#     hint_idx = load_progress_hint(st.session_state.cat, st.session_state.user)
#     st.session_state.idx = max(hint_idx, first_undecided_index_for(meta_for_init, comp_set_init))
#     st.session_state.idx_initialized_for = st.session_state.cat

# # ------------------------------ LEFT work area ------------------------------
# with left:
#     cfg = CAT[st.session_state.cat]
#     meta = load_meta(cfg["jsonl_id"])
#     if not meta:
#         st.warning("No records."); st.stop()

#     completed_set, log_h_map, log_a_map = build_completion_sets(cfg, st.session_state.user)

#     i = max(0, min(st.session_state.idx, len(meta)-1))
#     entry = meta[i]
#     hypo_name = entry.get("hypo_id", "")
#     adv_name  = entry.get("adversarial_id", "")
#     pk        = f"{hypo_name}|{adv_name}"

#     saved_h_row = (log_h_map.get(pk, {}) or {})
#     saved_a_row = (log_a_map.get(pk, {}) or {})
#     saved_h = (saved_h_row.get("status") or "").strip() or None
#     saved_a = (saved_a_row.get("status") or "").strip() or None
#     saved_h_copied_id = saved_h_row.get("copied_id")
#     saved_a_copied_id = saved_a_row.get("copied_id")

#     if pk not in st.session_state.dec:
#         st.session_state.dec[pk] = {"hypo": saved_h, "adv": saved_a}

#     st.markdown(f"### {entry.get('id','(no id)')} — <code>{pk}</code>", unsafe_allow_html=True)

#     # Text area: brief TEXT visible + HYPOTHESIS/ADVERSARIAL in collapsible expanders
#     st.markdown(f'**TEXT**: {entry.get("text","")}')
#     cexp1, cexp2 = st.columns(2)
#     with cexp1:
#         with st.expander("HYPOTHESIS (non-prototype) — show/hide", expanded=False):
#             st.markdown(f'<div class="small-text">{entry.get("hypothesis","")}</div>', unsafe_allow_html=True)
#     with cexp2:
#         with st.expander("ADVERSARIAL (prototype) — show/hide", expanded=False):
#             st.markdown(f'<div class="small-text">{entry.get("adversarial","")}</div>', unsafe_allow_html=True)

#     # Resolve Drive IDs
#     src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name)
#     src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)

#     imgL, imgR = st.columns(2, gap="large")

#     with imgL:
#         st.markdown("**Hypothesis (non-proto)**")
#         show_image(src_h_id, hypo_name, high_quality=st.session_state.hq)
#         b1, b2 = st.columns(2)
#         with b1:
#             if st.button("✅ Accept (hypo)", key=f"acc_h_{pk}"):
#                 st.session_state.dec[pk]["hypo"] = "accepted"
#         with b2:
#             if st.button("❌ Reject (hypo)", key=f"rej_h_{pk}"):
#                 st.session_state.dec[pk]["hypo"] = "rejected"
#         cur_h = st.session_state.dec[pk]["hypo"]
#         st.markdown(f'<div class="caption">Current: <b>{cur_h if cur_h else "—"}</b> | '
#                     f'Saved: <b>{saved_h or "—"}</b></div>', unsafe_allow_html=True)

#     with imgR:
#         st.markdown("**Adversarial (proto)**")
#         show_image(src_a_id, adv_name, high_quality=st.session_state.hq)
#         b3, b4 = st.columns(2)
#         with b3:
#             if st.button("✅ Accept (adv)", key=f"acc_a_{pk}"):
#                 st.session_state.dec[pk]["adv"] = "accepted"
#         with b4:
#             if st.button("❌ Reject (adv)", key=f"rej_a_{pk}"):
#                 st.session_state.dec[pk]["adv"] = "rejected"
#         cur_a = st.session_state.dec[pk]["adv"]
#         st.markdown(f'<div class="caption">Current: <b>{cur_a if cur_a else "—"}</b> | '
#                     f'Saved: <b>{saved_a or "—"}</b></div>', unsafe_allow_html=True)

#     st.markdown("<hr/>", unsafe_allow_html=True)

#         # ---------- SAVE & NAV (overwrite-safe + cleanup, no rerun in callback) ----------
#     def save_now():
#         st.session_state.saving = True

#         dec = st.session_state.dec[pk]
#         cur_h, cur_a = dec.get("hypo"), dec.get("adv")
#         if cur_h not in {"accepted", "rejected"} or cur_a not in {"accepted", "rejected"}:
#             # no banner at top; keep UI calm
#             st.session_state.saving = False
#             st.session_state.last_save_flash = {"msg": "Decide both sides before saving.", "ok": False, "ts": time.time()}
#             return

#         ts  = int(time.time())
#         base = dict(entry)
#         base["pair_key"]  = pk
#         base["annotator"] = st.session_state.user
#         base["_annotator_canon"] = canonical_user(st.session_state.user)

#         new_h_status, new_a_status = cur_h, cur_a

#         prev_h_copied = saved_h_copied_id
#         prev_a_copied = saved_a_copied_id
#         new_h_copied  = prev_h_copied
#         new_a_copied  = prev_a_copied

#         try:
#             # HYPOTHESIS shortcuts (flip-safe)
#             if saved_h == "accepted" and new_h_status != "accepted":
#                 delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg["dst_hypo"], hypo_name))
#                 new_h_copied = None
#             if new_h_status == "accepted":
#                 delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg["dst_hypo"], hypo_name))
#                 if src_h_id:
#                     new_h_copied = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg["dst_hypo"])

#             # ADVERSARIAL shortcuts (flip-safe)
#             if saved_a == "accepted" and new_a_status != "accepted":
#                 delete_file_by_id(drive, prev_a_copied or find_file_id_in_folder(drive, cfg["dst_adv"], adv_name))
#                 new_a_copied = None
#             if new_a_status == "accepted":
#                 delete_file_by_id(drive, prev_a_copied or find_file_id_in_folder(drive, cfg["dst_adv"], adv_name))
#                 if src_a_id:
#                     new_a_copied = create_shortcut_to_file(drive, src_a_id, adv_name, cfg["dst_adv"])
#         except HttpError as e:
#             st.session_state.saving = False
#             st.session_state.last_save_flash = {"msg": f"Drive shortcut update failed: {e}", "ok": False, "ts": time.time()}
#             return

#         rec_h = dict(base); rec_h.update({"side":"hypothesis", "status": new_h_status, "decided_at": ts})
#         if new_h_copied: rec_h["copied_id"] = new_h_copied
#         rec_a = dict(base); rec_a.update({"side":"adversarial", "status": new_a_status, "decided_at": ts})
#         if new_a_copied: rec_a["copied_id"] = new_a_copied

#         token = hashlib.sha1(json.dumps(
#             {"pk":pk, "h":rec_h["status"], "a":rec_a["status"], "who":base["_annotator_canon"]}
#         ).encode()).hexdigest()
#         if st.session_state.last_save_token == token:
#             st.session_state.saving = False
#             st.session_state.last_save_flash = {"msg": "Already saved this exact decision.", "ok": True, "ts": time.time()}
#             return

#         try:
#             append_lines_to_drive_text(drive, cfg["log_hypo"], [json.dumps(rec_h, ensure_ascii=False) + "\n"])
#             append_lines_to_drive_text(drive, cfg["log_adv"],  [json.dumps(rec_a, ensure_ascii=False) + "\n"])
#         except Exception as e:
#             st.session_state.saving = False
#             st.session_state.last_save_flash = {"msg": f"Failed to append logs: {e}", "ok": False, "ts": time.time()}
#             return

#         # Invalidate only cached functions (no AttributeError)
#         try: load_meta.clear()
#         except: pass
#         try: load_latest_map_for_annotator.clear()
#         except: pass

#         st.session_state.last_save_token = token
#         st.session_state.saving = False

#         # Decide next index now (no st.rerun() here)
#         meta_local = load_meta(cfg["jsonl_id"])
#         completed_set_local, _, _ = build_completion_sets(cfg, st.session_state.user)
#         next_idx = first_undecided_index_for(meta_local, completed_set_local)
#         st.session_state.idx = next_idx
#         save_progress_hint(st.session_state.cat, st.session_state.user, next_idx)

#         # flash message to show UNDER the nav row, in green
#         st.session_state.last_save_flash = {"msg": "Saved.", "ok": True, "ts": time.time()}

#     # ========== NAV row — Prev | BIG RED Save | Next ==========
#     navL, navC, navR = st.columns([1, 4, 1])
#     with navL:
#         if st.button("⏮ Prev"):
#             st.session_state.idx = max(0, i-1)
#             save_progress_hint(st.session_state.cat, st.session_state.user, st.session_state.idx)
#             st.rerun()

#     cur = st.session_state.dec.get(pk, {})
#     can_save = (cur.get("hypo") in {"accepted", "rejected"}) and (cur.get("adv") in {"accepted", "rejected"})

#     with navC:
#         st.button("💾 Save", key="save_btn", type="primary",
#                   disabled=(st.session_state.saving or not can_save),
#                   on_click=save_now, use_container_width=True)

#     with navR:
#         if st.button("Next ⏭"):
#             st.session_state.idx = min(len(meta)-1, i+1)
#             save_progress_hint(st.session_state.cat, st.session_state.user, st.session_state.idx)
#             st.rerun()

#     # ---- Flash area directly UNDER Prev | Save | Next ----
#     flash = st.session_state.get("last_save_flash")
#     if flash:
#         if flash.get("ok"):
#             st.success(flash["msg"])
#         else:
#             st.error(flash["msg"])

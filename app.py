# app.py — single-page, retry-hardened, overwrite-safe, compact UI (jump-safe & stable)
import io, json, time, hashlib, ssl, re, random
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

# ---------- Compact CSS + BIG red Save ----------
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
div[data-testid="stButton"] button[k="save_btn"],
div[data-testid="stButton"] button:where(.primary) {
  background-color: #e11d48 !important; border-color: #e11d48 !important;
}
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

# =========================== Stability Helpers ===========================
def _now() -> float: return time.time()
def _jitter(base: float, j: float = 0.2) -> float: return base * (1.0 + random.uniform(-j, j))

def _init_state_defaults():
    ss = st.session_state
    ss.setdefault("_btn_last_click", {})          # per-button last click ts
    ss.setdefault("_global_cooldown_until", 0.0)  # global click cooloff
    ss.setdefault("_save_lock", False)            # single-flight save
    ss.setdefault("_io_lock", False)              # serialize drive writes
    ss.setdefault("cat", ss.get("allowed", ["demography"])[0])
    ss.setdefault("idx", 0)
    ss.setdefault("dec", {})
    ss.setdefault("hq", False)
    ss.setdefault("saving", False)
    ss.setdefault("last_save_token", None)
    ss.setdefault("idx_initialized_for", None)
    ss.setdefault("_jump_anchor_idx", None)       # remember last jump anchor
    ss.setdefault("_last_meta_len", 0)
_init_state_defaults()

def safe_button(label: str, key: str, min_interval: float = 0.9, global_interval: float = 0.6, **kwargs) -> bool:
    """Debounced button: allows only if per-button and global cooldowns passed."""
    clicked = st.button(label, key=key, **kwargs)
    if not clicked:
        return False
    now = _now()
    last = st.session_state._btn_last_click.get(key, 0.0)
    if now < max(last + _jitter(min_interval), st.session_state._global_cooldown_until):
        return False
    st.session_state._btn_last_click[key] = now
    st.session_state._global_cooldown_until = now + _jitter(global_interval)
    return True

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
    time.sleep(min(1.5 * (2 ** attempt), 6.0) * (1.0 + random.uniform(-0.15, 0.15)))

_inproc_text_cache: Dict[str, str] = {}

def _download_bytes_with_retry(drive, file_id: str, attempts: int = 6) -> bytes:
    last_err = None
    for i in range(attempts):
        try:
            req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
            buf = io.BytesIO()
            dl = MediaIoBaseDownload(buf, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
            buf.seek(0)
            return buf.read()
        except (HttpError, ssl.SSLError, ConnectionError, requests.RequestException) as e:
            last_err = e; _retry_sleep(i)
        except Exception as e:
            last_err = e; _retry_sleep(i)
    raise last_err if last_err else RuntimeError("Unknown download error")

def read_text_from_drive(drive, file_id: str) -> str:
    try:
        data = _download_bytes_with_retry(drive, file_id)
        text = data.decode("utf-8", errors="ignore")
        _inproc_text_cache[file_id] = text
        return text
    except Exception:
        cached = _inproc_text_cache.get(file_id)
        if cached is not None:
            st.info("Drive read hiccup — used cached contents.")
            return cached
        raise

def write_text_to_drive(drive, file_id: str, text: str, attempts: int = 4):
    # serialize writes
    if st.session_state._io_lock:
        time.sleep(0.25)
    st.session_state._io_lock = True
    try:
        last_err = None
        for i in range(attempts):
            try:
                media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")),
                                          mimetype="text/plain", resumable=False)
                drive.files().update(fileId=file_id, media_body=media,
                                     supportsAllDrives=True).execute()
                _inproc_text_cache[file_id] = text
                return
            except (HttpError, ssl.SSLError, ConnectionError, requests.RequestException) as e:
                last_err = e; _retry_sleep(i)
            except Exception as e:
                last_err = e; _retry_sleep(i)
        if last_err: raise last_err
    finally:
        st.session_state._io_lock = False

def append_lines_to_drive_text(drive, file_id: str, new_lines: List[str], retries: int = 3):
    for attempt in range(retries):
        try:
            prev = read_text_from_drive(drive, file_id)
            updated = prev + "".join(new_lines)
            write_text_to_drive(drive, file_id, updated)
            return
        except Exception:
            _retry_sleep(attempt)
    prev = _inproc_text_cache.get(file_id, "")
    updated = prev + "".join(new_lines)
    write_text_to_drive(drive, file_id, updated)

def find_file_id_in_folder(drive, folder_id: Optional[str], filename: str) -> Optional[str]:
    if not filename or not folder_id:
        return None
    try:
        q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
        resp = drive.files().list(
            q=q, spaces="drive", fields="files(id,name,mimeType,shortcutDetails)", pageSize=10,
            supportsAllDrives=True, includeItemsFromAllDrives=True, corpora="allDrives"
        ).execute()
        files = resp.get("files", [])
        return files[0]["id"] if files else None
    except Exception:
        return None

def delete_file_by_id(drive, file_id: Optional[str]):
    if not file_id: return
    try:
        drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except Exception:
        pass

def create_shortcut_to_file(drive, src_file_id: Optional[str], new_name: str, dest_folder_id: Optional[str]) -> Optional[str]:
    if not (src_file_id and dest_folder_id):
        return None
    try:
        meta = {
            "name": new_name,
            "mimeType": "application/vnd.google-apps.shortcut",
            "parents": [dest_folder_id],
            "shortcutDetails": {"targetId": src_file_id},
        }
        res = drive.files().create(body=meta, fields="id,name",
                                   supportsAllDrives=True).execute()
        return res.get("id")
    except Exception:
        return None

# ---------- Progress pointer helpers ----------
def progress_file_id_for(cat: str, who: str) -> Optional[str]:
    try:
        parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
        fname = f"progress_{canonical_user(who)}_{cat}.txt"
        q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
        resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
                                  supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = resp.get("files", [])
        if files: return files[0]["id"]
        media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
        meta = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
        res = drive.files().create(body=meta, media_body=media, fields="id",
                                   supportsAllDrives=True).execute()
        return res["id"]
    except Exception:
        return None

def persist_pointer(cat: str, who: str, idx: int):
    pfid = progress_file_id_for(cat, who)
    if pfid is None: return
    try:
        write_text_to_drive(drive, pfid, str(int(max(0, idx))))
    except Exception:
        pass

# ================== Thumbnails / Full-res ===================
@st.cache_data(show_spinner=False, max_entries=512, ttl=3600)
def drive_thumbnail_bytes(file_id: str) -> Optional[bytes]:
    try:
        drv = get_drive()
        meta = drv.files().get(fileId=file_id, fields="thumbnailLink",
                               supportsAllDrives=True).execute()
        url = meta.get("thumbnailLink")
        if not url: return None
        r = requests.get(url, timeout=10)
        if r.ok: return r.content
    except Exception:
        pass
    return None

@st.cache_data(show_spinner=False, max_entries=256, ttl=3600)
def preview_bytes(file_id: str, max_side: int = 680) -> bytes:
    tb = drive_thumbnail_bytes(file_id)
    src = tb if tb is not None else _download_bytes_with_retry(get_drive(), file_id)
    with Image.open(io.BytesIO(src)) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side))
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=88, optimize=True)
        return out.getvalue()

@st.cache_data(show_spinner=False, max_entries=128, ttl=1800)
def original_bytes(file_id: str) -> bytes:
    return _download_bytes_with_retry(get_drive(), file_id)

def show_image(file_id: Optional[str], caption: str, high_quality: bool):
    if not file_id:
        st.warning(f"Missing image: {caption}"); return
    try:
        data = original_bytes(file_id) if high_quality else preview_bytes(file_id)
        st.image(data, caption=caption, use_container_width=True)
    except Exception as e:
        st.warning(f"Failed to render {caption}: {e}")

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
        "log_adv":  st.secrets["gcp"]["objects_adv_filtered_log_id"],  # fixed
        "hypo_prefix": "obj_h", "adv_prefix":  "obj_ah",
    },
}

# ===================== Readers / progress ======================
def canonical_user(name: str) -> str:
    return (name or "").strip().lower()

@st.cache_data(show_spinner=False)
def load_meta(jsonl_id: str) -> List[Dict[str, Any]]:
    try:
        drive.files().get(fileId=jsonl_id, fields="id", supportsAllDrives=True).execute()
    except HttpError as e:
        st.error(f"Cannot access JSONL file: {e}"); return []
    try:
        raw = read_text_from_drive(drive, jsonl_id)
    except Exception as e:
        st.error(f"Failed to read JSONL: {e}"); return []
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
    try:
        rows = latest_rows(read_text_from_drive(drive, log_file_id))
    except Exception:
        rows = []
    target = canonical_user(who)
    m: Dict[str, Dict] = {}
    for r in rows:
        pk = r.get("pair_key") or f"{r.get('hypo_id','')}|{r.get('adversarial_id','')}"
        r["pair_key"] = pk
        ann = canonical_user(r.get("annotator") or r.get("_annotator_canon") or "")
        if not ann:
            ann = target; r["annotator"] = who
        if ann == target:
            m[pk] = r
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

@st.cache_data(show_spinner=False)
def count_records_for_annotator(log_file_id: str, who: str) -> int:
    try:
        text = read_text_from_drive(drive, log_file_id)
    except Exception:
        return 0
    who_c = canonical_user(who)
    cnt = 0
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            r = json.loads(ln)
        except Exception:
            continue
        ann = canonical_user(r.get("annotator") or r.get("_annotator_canon") or "")
        if not ann:
            ann = who_c
        if ann == who_c:
            cnt += 1
    return cnt

def first_undecided_index_from_counts(meta_len: int, hypo_log_id: str, adv_log_id: str, who: str) -> int:
    h = count_records_for_annotator(hypo_log_id, who)
    a = count_records_for_annotator(adv_log_id, who)
    done = min(h, a)
    # clamp into 0..meta_len-1
    if meta_len <= 0: return 0
    return min(max(done, 0), meta_len - 1)

# ---------- Jump helpers ----------
def rows_per_prompt() -> int:
    try:
        return int(st.secrets.get("app", {}).get("rows_per_prompt", 5))
    except Exception:
        return 5

def parse_prompt_number(prompt_id: str) -> Optional[int]:
    """Extract the trailing integer; returns None if not found or <=0."""
    if not prompt_id:
        return None
    m = re.search(r"(\d+)$", str(prompt_id).strip())
    if not m:
        return None
    n = int(m.group(1))
    return n if n > 0 else None

def prompt_to_first_row_idx(n: int, total_len: int) -> int:
    """0-based first row of prompt n, with clamping to dataset size."""
    R = rows_per_prompt()
    idx0 = (n - 1) * R  # FIRST triplet of that prompt
    if total_len <= 0: return 0
    return min(max(idx0, 0), total_len - 1)

def clamp_idx(idx: int, total_len: int) -> int:
    if total_len <= 0: return 0
    return min(max(idx, 0), total_len - 1)

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
        st.session_state._jump_anchor_idx = None

    who = st.session_state.user
    cfg  = CAT[st.session_state.cat]

    meta = load_meta(cfg["jsonl_id"])
    st.session_state._last_meta_len = len(meta)

    done_h = count_records_for_annotator(cfg["log_hypo"], who)
    done_a = count_records_for_annotator(cfg["log_adv"],  who)
    completed = min(done_h, done_a)
    total_pairs = len(meta)
    pending = max(0, total_pairs - completed)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total", total_pairs)
    c2.metric("Completed (you)", completed)
    c3.metric("Pending", pending)

    st.session_state.hq = st.toggle("High quality images", value=st.session_state.hq)

    # ===== Jump to Prompt (stable) =====
    st.markdown("### Jump to Prompt")
    jcol1, jcol2 = st.columns([2, 1])
    jump_value = jcol1.text_input(
        "Enter prompt id or number (e.g., dem_00178 or 178)",
        value="", key="jump_prompt_input", label_visibility="collapsed"
    )
    jcol2.caption(f"Rows/prompt: {rows_per_prompt()}")

    if safe_button("Go", key="jump_go_btn", min_interval=0.8, global_interval=0.5):
        if not meta:
            st.warning("No records to jump.")
        else:
            n = parse_prompt_number(jump_value)
            if n is None:
                st.warning("Not a valid prompt id/number.")
            else:
                base_idx = prompt_to_first_row_idx(n, len(meta))
                st.session_state.idx = base_idx
                st.session_state._jump_anchor_idx = base_idx  # remember anchor
                persist_pointer(st.session_state.cat, st.session_state.user, base_idx)
                st.rerun()

# ---------- Auto-jump on first load using counts ----------
if st.session_state.idx_initialized_for != st.session_state.cat:
    meta_init = load_meta(CAT[st.session_state.cat]["jsonl_id"])
    idx = first_undecided_index_from_counts(len(meta_init), CAT[st.session_state.cat]["log_hypo"], CAT[st.session_state.cat]["log_adv"], st.session_state.user)
    st.session_state.idx = clamp_idx(idx, len(meta_init))
    st.session_state.idx_initialized_for = st.session_state.cat

# ------------------------------ LEFT work area ------------------------------
with left:
    cfg = CAT[st.session_state.cat]
    meta = load_meta(cfg["jsonl_id"])
    if not meta:
        st.warning("No records."); st.stop()

    # ensure idx valid (especially after any jump)
    st.session_state.idx = clamp_idx(st.session_state.idx, len(meta))
    i = st.session_state.idx

    completed_set, log_h_map, log_a_map = build_completion_sets(cfg, st.session_state.user)

    # guard against bad rows
    entry = meta[i] if 0 <= i < len(meta) else {}
    hypo_name = entry.get("hypo_id", "")
    adv_name  = entry.get("adversarial_id", "")
    pk        = f"{hypo_name}|{adv_name}" if (hypo_name or adv_name) else f"row_{i}"

    saved_h_row = (log_h_map.get(pk, {}) or {})
    saved_a_row = (log_a_map.get(pk, {}) or {})
    saved_h = (saved_h_row.get("status") or "").strip() or None
    saved_a = (saved_a_row.get("status") or "").strip() or None
    saved_h_copied_id = saved_h_row.get("copied_id")
    saved_a_copied_id = saved_a_row.get("copied_id")

    if pk not in st.session_state.dec:
        st.session_state.dec[pk] = {"hypo": saved_h, "adv": saved_a}

    st.markdown(f"### {entry.get('id','(no id)')} — <code>{pk}</code>", unsafe_allow_html=True)

    st.markdown(f'**TEXT**: {entry.get("text","")}')
    cexp1, cexp2 = st.columns(2)
    with cexp1:
        with st.expander("HYPOTHESIS (non-prototype) — show/hide", expanded=False):
            st.markdown(f'<div class="small-text">{entry.get("hypothesis","")}</div>', unsafe_allow_html=True)
    with cexp2:
        with st.expander("ADVERSARIAL (prototype) — show/hide", expanded=False):
            st.markdown(f'<div class="small-text">{entry.get("adversarial","")}</div>', unsafe_allow_html=True)

    src_h_id = find_file_id_in_folder(drive, cfg.get("src_hypo"), hypo_name)
    src_a_id = find_file_id_in_folder(drive, cfg.get("src_adv"),  adv_name)

    imgL, imgR = st.columns(2, gap="large")

    with imgL:
        st.markdown("**Hypothesis (non-proto)**")
        show_image(src_h_id, hypo_name or "(missing hypo)", high_quality=st.session_state.hq)
        b1, b2 = st.columns(2)
        with b1:
            acc_key = f"acc_h_{pk}"
            if safe_button("✅ Accept (hypo)", key=acc_key, min_interval=0.7, global_interval=0.5):
                st.session_state.dec[pk]["hypo"] = "accepted"
        with b2:
            rej_key = f"rej_h_{pk}"
            if safe_button("❌ Reject (hypo)", key=rej_key, min_interval=0.7, global_interval=0.5):
                st.session_state.dec[pk]["hypo"] = "rejected"
        cur_h = st.session_state.dec[pk]["hypo"]
        st.markdown(f'<div class="caption">Current: <b>{cur_h if cur_h else "—"}</b> | '
                    f'Saved: <b>{saved_h or "—"}</b></div>', unsafe_allow_html=True)

    with imgR:
        st.markdown("**Adversarial (proto)**")
        show_image(src_a_id, adv_name or "(missing adv)", high_quality=st.session_state.hq)
        b3, b4 = st.columns(2)
        with b3:
            acca_key = f"acc_a_{pk}"
            if safe_button("✅ Accept (adv)", key=acca_key, min_interval=0.7, global_interval=0.5):
                st.session_state.dec[pk]["adv"] = "accepted"
        with b4:
            reja_key = f"rej_a_{pk}"
            if safe_button("❌ Reject (adv)", key=reja_key, min_interval=0.7, global_interval=0.5):
                st.session_state.dec[pk]["adv"] = "rejected"
        cur_a = st.session_state.dec[pk]["adv"]
        st.markdown(f'<div class="caption">Current: <b>{cur_a if cur_a else "—"}</b> | '
                    f'Saved: <b>{saved_a or "—"}</b></div>', unsafe_allow_html=True)

    st.markdown("<hr/>", unsafe_allow_html=True)

    # ---------- SAVE & NAV ----------
    def save_now():
        if st.session_state._save_lock or st.session_state.saving:
            return
        st.session_state._save_lock = True
        st.session_state.saving = True
        try:
            dec = st.session_state.dec.get(pk, {})
            cur_h, cur_a = dec.get("hypo"), dec.get("adv")
            if cur_h not in {"accepted", "rejected"} or cur_a not in {"accepted", "rejected"}:
                st.session_state.last_save_flash = {"msg": "Decide both sides before saving.", "ok": False, "ts": _now()}
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
                if (saved_h == "accepted") and (new_h_status != "accepted"):
                    delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg.get("dst_hypo"), hypo_name))
                    new_h_copied = None
                if new_h_status == "accepted":
                    delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg.get("dst_hypo"), hypo_name))
                    if src_h_id:
                        new_h_copied = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg.get("dst_hypo"))

                # ADVERSARIAL shortcuts (flip-safe)
                if (saved_a == "accepted") and (new_a_status != "accepted"):
                    delete_file_by_id(drive, prev_a_copied or find_file_id_in_folder(drive, cfg.get("dst_adv"), adv_name))
                    new_a_copied = None
                if new_a_status == "accepted":
                    delete_file_by_id(drive, prev_a_copied or find_file_id_in_folder(drive, cfg.get("dst_adv"), adv_name))
                    if src_a_id:
                        new_a_copied = create_shortcut_to_file(drive, src_a_id, adv_name, cfg.get("dst_adv"))
            except HttpError as e:
                st.session_state.last_save_flash = {"msg": f"Drive shortcut update failed: {e}", "ok": False, "ts": _now()}
                return
            except Exception as e:
                st.session_state.last_save_flash = {"msg": f"Drive op failed: {e}", "ok": False, "ts": _now()}
                return

            rec_h = dict(base); rec_h.update({"side":"hypothesis", "status": new_h_status, "decided_at": ts})
            if new_h_copied: rec_h["copied_id"] = new_h_copied
            rec_a = dict(base); rec_a.update({"side":"adversarial", "status": new_a_status, "decided_at": ts})
            if new_a_copied: rec_a["copied_id"] = new_a_copied

            token = hashlib.sha1(json.dumps(
                {"pk":pk, "h":rec_h["status"], "a":rec_a["status"], "who":base["_annotator_canon"]}
            ).encode()).hexdigest()
            if st.session_state.last_save_token == token:
                st.session_state.last_save_flash = {"msg": "Already saved this exact decision.", "ok": True, "ts": _now()}
                return

            try:
                append_lines_to_drive_text(drive, cfg["log_hypo"], [json.dumps(rec_h, ensure_ascii=False) + "\n"])
                append_lines_to_drive_text(drive, cfg["log_adv"],  [json.dumps(rec_a, ensure_ascii=False) + "\n"])
            except Exception as e:
                st.session_state.last_save_flash = {"msg": f"Failed to append logs: {e}", "ok": False, "ts": _now()}
                return

            # Invalidate caches
            try: load_meta.clear()
            except: pass
            try: load_latest_map_for_annotator.clear()
            except: pass
            try: count_records_for_annotator.clear()
            except: pass

            st.session_state.last_save_token = token

            # Move to next pair safely
            try:
                meta_local = load_meta(cfg["jsonl_id"])
                st.session_state.idx = clamp_idx(i + 1, len(meta_local))
            except Exception:
                st.session_state.idx = clamp_idx(i + 1, len(meta))

            persist_pointer(st.session_state.cat, st.session_state.user, st.session_state.idx)
            st.session_state.last_save_flash = {"msg": "Saved.", "ok": True, "ts": _now()}

        finally:
            st.session_state.saving = False
            st.session_state._save_lock = False

    navL, navC, navR = st.columns([1, 4, 1])
    with navL:
        prev_key = f"prev_{pk}"
        if safe_button("⏮ Prev", key=prev_key, min_interval=0.5, global_interval=0.4):
            st.session_state.idx = clamp_idx(i - 1, len(meta))
            persist_pointer(st.session_state.cat, st.session_state.user, st.session_state.idx)
            st.rerun()

    cur = st.session_state.dec.get(pk, {})
    can_save = (cur.get("hypo") in {"accepted", "rejected"}) and (cur.get("adv") in {"accepted", "rejected"})
    with navC:
        save_key = f"save_{pk}"
        disabled_save = (st.session_state.saving or not can_save)
        if safe_button("💾 Save", key="save_btn", min_interval=1.0, global_interval=0.7, type="primary", disabled=disabled_save, use_container_width=True):
            with st.spinner("Saving..."):
                save_now()

    with navR:
        next_key = f"next_{pk}"
        if safe_button("Next ⏭", key=next_key, min_interval=0.5, global_interval=0.4):
            # if we jumped earlier, we still just go to i+1; anchor is only for the initial jump
            st.session_state.idx = clamp_idx(i + 1, len(meta))
            persist_pointer(st.session_state.cat, st.session_state.user, st.session_state.idx)
            st.rerun()

    flash = st.session_state.get("last_save_flash")
    if flash:
        if flash.get("ok"):
            st.success(flash["msg"])
        else:
            st.error(flash["msg"])
          

# # app.py — single-page, retry-hardened, overwrite-safe, compact UI
# import io, json, time, hashlib, ssl, re
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
# img {max-height: 500px; object-fit: contain;}
# hr {margin: 0.5rem 0;}
# div[data-testid="stButton"] button[k="save_btn"],
# div[data-testid="stButton"] button:where(.primary) {
#   background-color: #e11d48 !important; border-color: #e11d48 !important;
# }
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
#         pass

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

# # ---- Click throttle ----
# def _cooldown_key(action_key: str) -> str:
#     return f"_next_ok_{action_key}"

# def cooldown_disabled(action_key: str) -> bool:
#     import time
#     return time.time() < st.session_state.get(_cooldown_key(action_key), 0.0)

# def cooldown_start(action_key: str, seconds: float = 0.8):
#     import time
#     st.session_state[_cooldown_key(action_key)] = time.time() + seconds

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
#         "log_adv":  st.secrets["gcp"]["objects_hypo_filtered_log_id"],  # <- ensure correct secret
#         "hypo_prefix": "obj_h", "adv_prefix":  "obj_ah",
#     },
# }

# # ===================== Readers / progress ======================
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
#             m[pk] = r
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

# @st.cache_data(show_spinner=False)
# def count_records_for_annotator(log_file_id: str, who: str) -> int:
#     text = read_text_from_drive(drive, log_file_id)
#     who_c = canonical_user(who)
#     cnt = 0
#     for ln in text.splitlines():
#         ln = ln.strip()
#         if not ln:
#             continue
#         try:
#             r = json.loads(ln)
#         except Exception:
#             continue
#         ann = canonical_user(r.get("annotator") or r.get("_annotator_canon") or "")
#         if not ann:
#             ann = who_c
#         if ann == who_c:
#             cnt += 1
#     return cnt

# def first_undecided_index_from_counts(meta_len: int, hypo_log_id: str, adv_log_id: str, who: str) -> int:
#     h = count_records_for_annotator(hypo_log_id, who)
#     a = count_records_for_annotator(adv_log_id, who)
#     done = min(h, a)
#     return min(done, max(0, meta_len - 1))

# # ---------- Jump helpers ----------
# def rows_per_prompt() -> int:
#     try:
#         return int(st.secrets.get("app", {}).get("rows_per_prompt", 5))
#     except Exception:
#         return 5

# def prompt_to_base_index(prompt_id: str, total_len: int) -> int:
#     """
#     Map prompt id (e.g., 'dem_00033' or '33') to the FIRST row of that prompt.
#     If there are R rows per prompt, prompt n starts at index: (n - 1) * R  (0-based).
#     """
#     if not prompt_id:
#         return 0
#     m = re.search(r"(\d+)$", str(prompt_id).strip())
#     if not m:
#         return 0
#     n = int(m.group(1))
#     R = rows_per_prompt()  # typically 5
#     idx0 = (n - 1) * R                  # <-- first row of prompt n (0-based)
#     if idx0 < 0:
#         idx0 = 0
#     if total_len > 0:
#         idx0 = min(idx0, total_len - 1) # clamp
#     return idx0

# # ========================= UI state =========================
# if "cat"  not in st.session_state: st.session_state.cat  = st.session_state.allowed[0]
# if "idx"  not in st.session_state: st.session_state.idx  = 0
# if "dec"  not in st.session_state: st.session_state.dec  = {}
# if "hq"   not in st.session_state: st.session_state.hq   = False
# if "saving" not in st.session_state: st.session_state.saving = False
# if "last_save_token" not in st.session_state: st.session_state.last_save_token = None
# if "idx_initialized_for" not in st.session_state: st.session_state.idx_initialized_for = None
# if "jump_mode" not in st.session_state: st.session_state.jump_mode = False  # <— NEW

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
#         st.session_state.jump_mode = False  # reset jump mode on category change

#     who = st.session_state.user
#     cfg  = CAT[st.session_state.cat]
#     meta = load_meta(cfg["jsonl_id"])

#     done_h = count_records_for_annotator(cfg["log_hypo"], who)
#     done_a = count_records_for_annotator(cfg["log_adv"],  who)
#     completed = min(done_h, done_a)
#     total_pairs = len(meta)
#     pending = max(0, total_pairs - completed)

#     c1, c2, c3 = st.columns(3)
#     c1.metric("Total", total_pairs)
#     c2.metric("Completed (you)", completed)
#     c3.metric("Pending", pending)

#     st.session_state.hq = st.toggle("High quality images", value=st.session_state.hq)

#     # ===== Jump to Prompt =====
#     st.markdown("### Jump to Prompt")
#     jcol1, jcol2 = st.columns([2, 1])
#     jump_value = jcol1.text_input(
#         "Enter prompt id or number (e.g., dem_00178 or 178)",
#         value="", key="jump_prompt_input", label_visibility="collapsed"
#     )
#     jcol2.caption(f"Rows/prompt: {rows_per_prompt()}")

#     if st.button("Go", key="jump_go_btn"):
#         if not meta:
#             st.warning("No records to jump.")
#         else:
#             base_idx = prompt_to_base_index(jump_value, len(meta))
#             st.session_state.idx = base_idx
#             st.session_state.jump_mode = True  # <— turn on jump mode
#             try:
#                 def progress_file_id_for(cat: str, who: str) -> str:
#                     parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
#                     fname = f"progress_{cat}_{canonical_user(who)}.txt"
#                     q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
#                     resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
#                                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
#                     files = resp.get("files", [])
#                     if files: return files[0]["id"]
#                     media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
#                     meta_tmp = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
#                     return drive.files().create(body=meta_tmp, media_body=media, fields="id",
#                                                 supportsAllDrives=True).execute()["id"]
#                 pfid = progress_file_id_for(st.session_state.cat, st.session_state.user)
#                 write_text_to_drive(drive, pfid, str(base_idx))
#             except Exception:
#                 pass
#             st.rerun()

# # ---------- Auto-jump on first load using counts ----------
# if st.session_state.idx_initialized_for != st.session_state.cat:
#     cfg_init  = CAT[st.session_state.cat]
#     meta_init = load_meta(cfg_init["jsonl_id"])
#     idx = first_undecided_index_from_counts(len(meta_init), cfg_init["log_hypo"], cfg_init["log_adv"], st.session_state.user)
#     st.session_state.idx = idx
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

#     st.markdown(f'**TEXT**: {entry.get("text","")}')
#     cexp1, cexp2 = st.columns(2)
#     with cexp1:
#         with st.expander("HYPOTHESIS (non-prototype) — show/hide", expanded=False):
#             st.markdown(f'<div class="small-text">{entry.get("hypothesis","")}</div>', unsafe_allow_html=True)
#     with cexp2:
#         with st.expander("ADVERSARIAL (prototype) — show/hide", expanded=False):
#             st.markdown(f'<div class="small-text">{entry.get("adversarial","")}</div>', unsafe_allow_html=True)

#     src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name)
#     src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)

#     imgL, imgR = st.columns(2, gap="large")

#     with imgL:
#         st.markdown("**Hypothesis (non-proto)**")
#         show_image(src_h_id, hypo_name, high_quality=st.session_state.hq)
#         b1, b2 = st.columns(2)
#         with b1:
#             acc_key = f"acc_h_{pk}"
#             if st.button("✅ Accept (hypo)", key=acc_key, disabled=cooldown_disabled(acc_key)):
#                 cooldown_start(acc_key)
#                 st.session_state.dec[pk]["hypo"] = "accepted"
#         with b2:
#             rej_key = f"rej_h_{pk}"
#             if st.button("❌ Reject (hypo)", key=rej_key, disabled=cooldown_disabled(rej_key)):
#                 cooldown_start(rej_key)
#                 st.session_state.dec[pk]["hypo"] = "rejected"
#         cur_h = st.session_state.dec[pk]["hypo"]
#         st.markdown(f'<div class="caption">Current: <b>{cur_h if cur_h else "—"}</b> | '
#                     f'Saved: <b>{saved_h or "—"}</b></div>', unsafe_allow_html=True)

#     with imgR:
#         st.markdown("**Adversarial (proto)**")
#         show_image(src_a_id, adv_name, high_quality=st.session_state.hq)
#         b3, b4 = st.columns(2)
#         with b3:
#             acca_key = f"acc_a_{pk}"
#             if st.button("✅ Accept (adv)", key=acca_key, disabled=cooldown_disabled(acca_key)):
#                 cooldown_start(acca_key)
#                 st.session_state.dec[pk]["adv"] = "accepted"
#         with b4:
#             reja_key = f"rej_a_{pk}"
#             if st.button("❌ Reject (adv)", key=reja_key, disabled=cooldown_disabled(reja_key)):
#                 cooldown_start(reja_key)
#                 st.session_state.dec[pk]["adv"] = "rejected"
#         cur_a = st.session_state.dec[pk]["adv"]
#         st.markdown(f'<div class="caption">Current: <b>{cur_a if cur_a else "—"}</b> | '
#                     f'Saved: <b>{saved_a or "—"}</b></div>', unsafe_allow_html=True)

#     st.markdown("<hr/>", unsafe_allow_html=True)

#     # ---------- SAVE & NAV ----------
#     def save_now():
#         st.session_state.saving = True

#         dec = st.session_state.dec[pk]
#         cur_h, cur_a = dec.get("hypo"), dec.get("adv")
#         if cur_h not in {"accepted", "rejected"} or cur_a not in {"accepted", "rejected"}:
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
#             if saved_h == "accepted" and new_h_status != "accepted":
#                 delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg["dst_hypo"], hypo_name))
#                 new_h_copied = None
#             if new_h_status == "accepted":
#                 delete_file_by_id(drive, prev_h_copied or find_file_id_in_folder(drive, cfg["dst_hypo"], hypo_name))
#                 if src_h_id:
#                     new_h_copied = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg["dst_hypo"])

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

#         try: load_meta.clear()
#         except: pass
#         try: load_latest_map_for_annotator.clear()
#         except: pass
#         try: count_records_for_annotator.clear()
#         except: pass

#         st.session_state.last_save_token = token
#         st.session_state.saving = False

#         # ---------- Where to go next ----------
#         meta_local = load_meta(cfg["jsonl_id"])
#         if st.session_state.get("jump_mode", False):
#             # stay in the jumped flow: just advance by one row
#             next_idx = min(st.session_state.idx + 1, max(0, len(meta_local) - 1))
#         else:
#             # default resume-from-progress behavior
#             done_h2 = count_records_for_annotator(cfg["log_hypo"], st.session_state.user)
#             done_a2 = count_records_for_annotator(cfg["log_adv"],  st.session_state.user)
#             next_idx = min(done_h2, done_a2)

#         st.session_state.idx = next_idx

#         # persist updated pointer (optional)
#         try:
#             def progress_file_id_for(cat: str, who: str) -> str:
#                 parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
#                 fname = f"progress_{cat}_{canonical_user(who)}.txt"
#                 q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
#                 resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
#                                           supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
#                 files = resp.get("files", [])
#                 if files: return files[0]["id"]
#                 media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
#                 meta_tmp = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
#                 return drive.files().create(body=meta_tmp, media_body=media, fields="id",
#                                             supportsAllDrives=True).execute()["id"]
#             pfid = progress_file_id_for(st.session_state.cat, st.session_state.user)
#             write_text_to_drive(drive, pfid, str(st.session_state.idx))
#         except Exception:
#             pass

#         st.session_state.last_save_flash = {"msg": "Saved.", "ok": True, "ts": time.time()}

#     navL, navC, navR = st.columns([1, 4, 1])
#     with navL:
#         prev_key = f"prev_{pk}"
#         if st.button("⏮ Prev", key=prev_key, disabled=cooldown_disabled(prev_key)):
#             cooldown_start(prev_key)
#             st.session_state.idx = max(0, i-1)
#             try:
#                 def progress_file_id_for(cat: str, who: str) -> str:
#                     parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
#                     fname = f"progress_{cat}_{canonical_user(who)}.txt"
#                     q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
#                     resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
#                                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
#                     files = resp.get("files", [])
#                     if files: return files[0]["id"]
#                     media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
#                     meta_tmp = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
#                     return drive.files().create(body=meta_tmp, media_body=media, fields="id",
#                                                 supportsAllDrives=True).execute()["id"]
#                 pfid = progress_file_id_for(st.session_state.cat, st.session_state.user)
#                 write_text_to_drive(drive, pfid, str(st.session_state.idx))
#             except Exception:
#                 pass
#             st.rerun()

#     cur = st.session_state.dec.get(pk, {})
#     can_save = (cur.get("hypo") in {"accepted", "rejected"}) and (cur.get("adv") in {"accepted", "rejected"})

#     with navC:
#         save_key = f"save_{pk}"
#         disabled_save = (st.session_state.saving or not can_save or cooldown_disabled(save_key))
#         if st.button("💾 Save", key="save_btn", type="primary", disabled=disabled_save, use_container_width=True):
#             cooldown_start(save_key)
#             save_now()

#     with navR:
#         next_key = f"next_{pk}"
#         if st.button("Next ⏭", key=next_key, disabled=cooldown_disabled(next_key)):
#             cooldown_start(next_key)
#             st.session_state.idx = min(len(meta)-1, i+1)
#             try:
#                 def progress_file_id_for(cat: str, who: str) -> str:
#                     parent = st.secrets["gcp"].get("progress_parent_id") or st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
#                     fname = f"progress_{cat}_{canonical_user(who)}.txt"
#                     q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
#                     resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
#                                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
#                     files = resp.get("files", [])
#                     if files: return files[0]["id"]
#                     media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
#                     meta_tmp = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
#                     return drive.files().create(body=meta_tmp, media_body=media, fields="id",
#                                                 supportsAllDrives=True).execute()["id"]
#                 pfid = progress_file_id_for(st.session_state.cat, st.session_state.user)
#                 write_text_to_drive(drive, pfid, str(st.session_state.idx))
#             except Exception:
#                 pass
#             st.rerun()

#     flash = st.session_state.get("last_save_flash")
#     if flash:
#         if flash.get("ok"):
#             st.success(flash["msg"])
#         else:
#             st.error(flash["msg"])

import io, json, time
from typing import Dict, Any, Optional
from PIL import Image
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

st.set_page_config(page_title="Image Triplet Filter", layout="wide")

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

def drive_download_bytes(drive, file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()

def find_file_id_in_folder(drive, folder_id: str, filename: str) -> Optional[str]:
    if not filename:
        return None
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = drive.files().list(
        q=q, spaces="drive",
        fields="files(id,name)", pageSize=1,
        supportsAllDrives=True, includeItemsFromAllDrives=True, corpora="allDrives"
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def create_shortcut_to_file(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
    meta = {
        "name": new_name,
        "mimeType": "application/vnd.google-apps.shortcut",
        "parents": [dest_folder_id],
        "shortcutDetails": {"targetId": src_file_id},
    }
    res = drive.files().create(
        body=meta, fields="id,name", supportsAllDrives=True
    ).execute()
    return res["id"]

def read_text_from_drive(drive, file_id: str) -> str:
    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read().decode("utf-8", errors="ignore")

def append_lines_to_drive_text(drive, file_id: str, new_lines: list[str]):
    prev = read_text_from_drive(drive, file_id)
    updated = prev + "".join(new_lines)
    media = MediaIoBaseUpload(io.BytesIO(updated.encode("utf-8")), mimetype="text/plain", resumable=False)
    drive.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()

def read_jsonl_from_drive(drive, file_id: str):
    # validate
    try:
        drive.files().get(fileId=file_id, fields="id", supportsAllDrives=True).execute()
    except HttpError as e:
        st.error(f"Cannot access JSONL file: {e}")
        st.stop()
    raw = read_text_from_drive(drive, file_id)
    out = []
    for ln in raw.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out
    
# Cache raw bytes (per file id) so Drive isn’t hit every rerun
@st.cache_data(show_spinner=False)
def get_drive_bytes_cached(file_id: str) -> bytes:
    drv = get_drive()
    return drive_download_bytes(drv, file_id)  # your existing function

# Create and cache a small preview to save RAM/CPU
@st.cache_data(show_spinner=False)
def make_preview_bytes(file_id: str, max_side: int = 900) -> bytes:
    raw = get_drive_bytes_cached(file_id)
    # Pillow decode -> downscale -> JPEG
    with Image.open(io.BytesIO(raw)) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side))  # in-place, preserves aspect
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=85, optimize=True)
        return buf.getvalue()

# =========================== Category config ===========================

CAT = {
    "demography": {
        "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["demography_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],   # shortcuts (accepted)
        "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],    # shortcuts (accepted)
        "log_hypo": st.secrets["gcp"]["demography_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["demography_adv_filtered_log_id"],
        "hypo_prefix": "dem_h",
        "adv_prefix":  "dem_ah",
    },
    "animal": {
        "jsonl_id": st.secrets["gcp"]["animal_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["animal_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["animal_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["animal_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["animal_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["animal_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["animal_adv_filtered_log_id"],
        "hypo_prefix": "ani_h",
        "adv_prefix":  "ani_ah",
    },
    "objects": {
        "jsonl_id": st.secrets["gcp"]["objects_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["objects_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["objects_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["objects_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["objects_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["objects_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["objects_adv_filtered_log_id"],
        "hypo_prefix": "obj_h",
        "adv_prefix":  "obj_ah",
    },
}

drive = get_drive()

# ===================== Cached loaders (+dedupe) ======================

@st.cache_data(show_spinner=False)
def load_meta(jsonl_id: str):
    return read_jsonl_from_drive(drive, jsonl_id)

def _latest_by_pair(lines: list[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    From JSONL rows, keep only the latest per pair_key.
    We accept both old rows (without pair_key) and new rows (with pair_key).
    """
    m = {}
    for rec in lines:
        hypo = rec.get("hypo_id") or ""
        adv  = rec.get("adversarial_id") or ""
        pk   = rec.get("pair_key") or f"{hypo}|{adv}"
        rec["pair_key"] = pk
        m[pk] = rec  # last wins
    return m

@st.cache_data(show_spinner=False)
def load_map(file_id: str):
    rows = read_jsonl_from_drive(drive, file_id)
    return _latest_by_pair(rows)

# ========================= UI State =========================

if "page" not in st.session_state: st.session_state.page = "home"
if "cat"  not in st.session_state: st.session_state.cat  = None
if "idx"  not in st.session_state: st.session_state.idx  = 0
if "dec"  not in st.session_state: st.session_state.dec  = {}   # per-pair temp: dec[_pair_key] = {"hypo":..., "adv":...}
if "saving" not in st.session_state: st.session_state.saving = False

def go(p): st.session_state.page = p

# =========================== HOME ===========================

if st.session_state.page == "home":
    st.title("Image Triplet Filter")
    cat_pick = st.selectbox("Select category", list(CAT.keys()))
    if st.button("Continue ➜", type="primary", key="home_go"):
        st.session_state.cat = cat_pick
        st.session_state.idx = 0
        st.session_state.dec = {}
        st.session_state.saving = False
        go("dashboard")

# ========================= DASHBOARD =========================

elif st.session_state.page == "dashboard":
    st.button("⬅️ Back", on_click=lambda: go("home"), key="dash_back")
    cat = st.session_state.cat
    if cat is None:
        st.warning("Pick a category first.")
        go("home")
        st.stop()

    cfg  = CAT[cat]
    meta = load_meta(cfg["jsonl_id"])
    log_h = load_map(cfg["log_hypo"])
    log_a = load_map(cfg["log_adv"])

    # total pairs
    total_pairs = len(meta)

    # completed = both sides present for a pair_key
    def pair_key(e): return f"{e.get('hypo_id','')}|{e.get('adversarial_id','')}"
    completed = sum(1 for e in meta if (pair_key(e) in log_h and pair_key(e) in log_a))
    pending   = total_pairs - completed

    st.subheader(f"Category: **{cat}**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total pairs", total_pairs)
    c2.metric("Completed", completed)
    c3.metric("Pending", pending)

    # find first undecided pair
    def first_undecided_index():
        for i, e in enumerate(meta):
            pk = pair_key(e)
            if not (pk in log_h and pk in log_a):
                return i
        return 0

    if st.button("▶️ Start / Resume", type="primary", key="dash_start"):
        st.session_state.idx = first_undecided_index()
        st.session_state.saving = False
        go("review")

# ========================== REVIEW ==========================

elif st.session_state.page == "review":
    top_l, _ = st.columns([1,6])
    top_l.button("⬅️ Back", on_click=lambda: go("dashboard"), key="rev_back")

    cat = st.session_state.cat
    cfg = CAT[cat]
    meta = load_meta(cfg["jsonl_id"])
    if not meta:
        st.warning("No records.")
        st.stop()

    i = max(0, min(st.session_state.idx, len(meta)-1))
    entry = meta[i]

    # identity for this pair
    hypo_name = entry.get("hypo_id", "")
    adv_name  = entry.get("adversarial_id", "")
    pair_key  = f"{hypo_name}|{adv_name}"

    # pull saved status (latest per pair) for display
    saved_h = load_map(cfg["log_hypo"]).get(pair_key, {}).get("status")
    saved_a = load_map(cfg["log_adv"]).get(pair_key, {}).get("status")

    # init temp session decisions from saved
    if pair_key not in st.session_state.dec:
        st.session_state.dec[pair_key] = {"hypo": saved_h, "adv": saved_a}

    st.subheader(f"{entry.get('id','(no id)')}  —  {pair_key}")

    with st.expander("📝 Text / Descriptions", expanded=True):
        st.markdown(f"**TEXT**: {entry.get('text','')}")
        st.markdown(f"**HYPOTHESIS (non-prototype)**: {entry.get('hypothesis','')}")
        st.markdown(f"**ADVERSARIAL (prototype)**: {entry.get('adversarial','')}")

    # resolve Drive IDs
    src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name)
    src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)
    
    def show_img_preview(file_id: Optional[str], caption: str):
    if not file_id:
        st.error(f"Missing image: {caption}")
        return
    try:
        # uses the cached + downscaled preview you defined above
        st.image(make_preview_bytes(file_id), caption=caption, use_container_width=True)
    except Exception as e:
        # fallback to original bytes if Pillow fails
        st.warning(f"Preview failed ({e}); showing original.")
        st.image(get_drive_bytes_cached(file_id), caption=caption, use_container_width=True)

    c1, c2 = st.columns(2)

    # ---------- Hypothesis ----------
    with c1:
        st.markdown("**Hypothesis (non-proto)**")
        show_img_preview(src_h_id, hypo_name) 
        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Accept (hypo)", key=f"acc_h_{pair_key}", use_container_width=True):
                st.session_state.dec[pair_key]["hypo"] = "accepted"
        with b2:
            if st.button("❌ Reject (hypo)", key=f"rej_h_{pair_key}", use_container_width=True):
                st.session_state.dec[pair_key]["hypo"] = "rejected"
        st.caption(f"Current: {st.session_state.dec[pair_key]['hypo'] or '—'} | Saved: {saved_h or '—'}")

    # ---------- Adversarial ----------
    with c2:
        st.markdown("**Adversarial (proto)**")
        show_img_preview(src_a_id, adv_name) 
        b3, b4 = st.columns(2)
        with b3:
            if st.button("✅ Accept (adv)", key=f"acc_a_{pair_key}", use_container_width=True):
                st.session_state.dec[pair_key]["adv"] = "accepted"
        with b4:
            if st.button("❌ Reject (adv)", key=f"rej_a_{pair_key}", use_container_width=True):
                st.session_state.dec[pair_key]["adv"] = "rejected"
        st.caption(f"Current: {st.session_state.dec[pair_key]['adv'] or '—'} | Saved: {saved_a or '—'}")

    st.divider()

    # ---------- Save (idempotent; disables after click) ----------
    def save_now():
        st.session_state.saving = True
        dec = st.session_state.dec[pair_key]
        ts  = int(time.time())

        base = dict(entry)
        base["pair_key"] = pair_key

        rec_h = dict(base)
        rec_h.update({"side": "hypothesis", "status": dec.get("hypo") or "rejected", "decided_at": ts})
        rec_a = dict(base)
        rec_a.update({"side": "adversarial", "status": dec.get("adv") or "rejected", "decided_at": ts})

        try:
            # Only create shortcuts when accepted
            if dec.get("hypo") == "accepted" and src_h_id:
                rec_h["copied_id"] = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg["dst_hypo"])
            if dec.get("adv") == "accepted" and src_a_id:
                rec_a["copied_id"] = create_shortcut_to_file(drive, src_a_id,  adv_name, cfg["dst_adv"])
        except HttpError as e:
            st.error(f"Drive shortcut failed:\n{e}")
            st.session_state.saving = False
            return

        try:
            # append both lines in one go
            append_lines_to_drive_text(drive, cfg["log_hypo"], [json.dumps(rec_h, ensure_ascii=False) + "\n"])
            append_lines_to_drive_text(drive, cfg["log_adv"],  [json.dumps(rec_a, ensure_ascii=False) + "\n"])
        except HttpError as e:
            st.error(f"Failed to append logs: {e}")
            st.session_state.saving = False
            return

        # update caches incrementally (no full reload needed to reflect Saved)
        load_map.clear()  # small logs -> ok to reload on next display
        st.success("Saved.")
        st.session_state.saving = False

    nav_l, save_c, nav_r = st.columns([1,2,1])
    with nav_l:
        if st.button("⏮ Prev", key=f"prev_{pair_key}", use_container_width=True):
            st.session_state.idx = max(0, i-1)
            st.rerun()

    with save_c:
        st.button("💾 Save", type="primary", key=f"save_{pair_key}", use_container_width=True, disabled=st.session_state.saving, on_click=save_now)

    with nav_r:
        if st.button("Next ⏭", key=f"next_{pair_key}", use_container_width=True):
            st.session_state.idx = min(len(meta)-1, i+1)
            st.rerun()

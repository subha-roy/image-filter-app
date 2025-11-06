# app.py
import io, json, time, hashlib
from typing import Dict, Any, Optional, List, Tuple
import requests
from PIL import Image
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# -------- Safe image defaults --------
Image.MAX_IMAGE_PIXELS = 80_000_000  # large but safe

st.set_page_config(page_title="Image Triplet Filter", layout="wide")

# =========================== Auth ============================
USERS = {
    "Subhadeep": {"password": "Ado1234", "categories": ["demography"]},
    "Gagan":     {"password": "Ado1234", "categories": ["animal"]},
    "Robustness":{"password": "Ado1234", "categories": ["demography", "animal", "objects"]},
}

def do_login_ui():
    st.title("Image Triplet Filter – Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    ok = st.button("Sign in", type="primary")
    if ok:
        info = USERS.get(u)
        if info and info["password"] == p:
            st.session_state.user = u
            st.session_state.allowed = info["categories"]
            st.session_state.page = "home"
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
        # normalize newlines if pasted raw
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

def _download_bytes(drive, file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()

def read_text_from_drive(drive, file_id: str) -> str:
    return _download_bytes(drive, file_id).decode("utf-8", errors="ignore")

def append_lines_to_drive_text(drive, file_id: str, new_lines: List[str], retries: int = 3):
    # optimistic append with small retry (handles concurrent writers)
    for attempt in range(retries):
        try:
            prev = read_text_from_drive(drive, file_id)
            updated = prev + "".join(new_lines)
            media = MediaIoBaseUpload(io.BytesIO(updated.encode("utf-8")),
                                      mimetype="text/plain", resumable=False)
            drive.files().update(fileId=file_id, media_body=media,
                                 supportsAllDrives=True).execute()
            return
        except HttpError:
            time.sleep(0.4 * (attempt + 1))
    # last attempt without merge (still better than losing data)
    media = MediaIoBaseUpload(io.BytesIO("".join(new_lines).encode("utf-8")),
                              mimetype="text/plain", resumable=False)
    drive.files().update(fileId=file_id, media_body=media,
                         supportsAllDrives=True).execute()

def find_file_id_in_folder(drive, folder_id: str, filename: str) -> Optional[str]:
    if not filename:
        return None
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = drive.files().list(
        q=q, spaces="drive", fields="files(id,name)", pageSize=1,
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
    res = drive.files().create(body=meta, fields="id,name",
                               supportsAllDrives=True).execute()
    return res["id"]

# ================== Thumbnails + Full-res (toggle) ===================
@st.cache_data(show_spinner=False, max_entries=512, ttl=3600)
def drive_thumbnail_bytes(file_id: str) -> Optional[bytes]:
    drv = get_drive()
    try:
        meta = drv.files().get(
            fileId=file_id, fields="thumbnailLink", supportsAllDrives=True
        ).execute()
        url = meta.get("thumbnailLink")
        if not url:
            return None
        r = requests.get(url, timeout=10)
        if r.ok:
            return r.content
    except Exception:
        pass
    return None

@st.cache_data(show_spinner=False, max_entries=256, ttl=3600)
def preview_bytes(file_id: str, max_side: int = 900) -> bytes:
    tb = drive_thumbnail_bytes(file_id)
    src = tb if tb is not None else _download_bytes(get_drive(), file_id)
    with Image.open(io.BytesIO(src)) as im:
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side))
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=90, optimize=True)
        return out.getvalue()

@st.cache_data(show_spinner=False, max_entries=128, ttl=1800)
def original_bytes(file_id: str) -> bytes:
    return _download_bytes(get_drive(), file_id)

def show_image(file_id: Optional[str], caption: str, high_quality: bool):
    if not file_id:
        st.error(f"Missing image: {caption}")
        return
    try:
        data = original_bytes(file_id) if high_quality else preview_bytes(file_id)
        st.image(data, caption=caption, use_container_width=True)
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

# ===================== Readers / progress from LOGS ======================
def canonical_user(name: str) -> str:
    return (name or "").strip().lower()

@st.cache_data(show_spinner=False)
def load_meta(jsonl_id: str) -> List[Dict[str, Any]]:
    # validate & read
    try:
        drive.files().get(fileId=jsonl_id, fields="id",
                          supportsAllDrives=True).execute()
    except HttpError as e:
        st.error(f"Cannot access JSONL file: {e}")
        st.stop()
    raw = read_text_from_drive(drive, jsonl_id)
    out: List[Dict[str, Any]] = []
    for ln in raw.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out

def latest_rows(jsonl_text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ln in jsonl_text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out

def load_latest_map_for_annotator(log_file_id: str, who: str) -> Dict[str, Dict]:
    """Return {pair_key: latest_row} only for this annotator (canonical)."""
    rows = latest_rows(read_text_from_drive(drive, log_file_id))
    target = canonical_user(who)
    m: Dict[str, Dict] = {}
    for r in rows:
        pk = r.get("pair_key") or f"{r.get('hypo_id','')}|{r.get('adversarial_id','')}"
        r["pair_key"] = pk
        ann = canonical_user(r.get("annotator") or r.get("_annotator_canon") or "")
        if not ann:
            ann = target  # back-compat for old rows with no annotator
            r["annotator"] = who
        if ann == target:
            m[pk] = r  # last wins
    return m

def build_completion_sets(cat_cfg: dict, who: str) -> Tuple[set, Dict[str, Dict], Dict[str, Dict]]:
    """Return (completed_set, hypo_map, adv_map) for this annotator."""
    log_h_map = load_latest_map_for_annotator(cat_cfg["log_hypo"], who)
    log_a_map = load_latest_map_for_annotator(cat_cfg["log_adv"],  who)
    completed = set()
    for pk in set(log_h_map.keys()) | set(log_a_map.keys()):
        s_h = (log_h_map.get(pk, {}).get("status") or "").strip()
        s_a = (log_a_map.get(pk, {}).get("status") or "").strip()
        if s_h and s_a:  # any decision (accepted/rejected) counts as completed
            completed.add(pk)
    return completed, log_h_map, log_a_map

def pk_of(e: Dict[str, Any]) -> str:
    return f"{e.get('hypo_id','')}|{e.get('adversarial_id','')}"

def first_undecided_index_for(meta: List[Dict[str, Any]], completed_set: set) -> int:
    for i, e in enumerate(meta):
        if pk_of(e) not in completed_set:
            return i
    return max(0, len(meta) - 1)

# -------- Optional progress pointer file (kept, but not trusted) ----------
def progress_file_id_for(cat: str, who: str) -> str:
    parent = st.secrets["gcp"].get("progress_parent_id")
    if not parent:
        parent = st.secrets["gcp"][f"{cat}_hypo_filtered_log_id"]
    fname = f"progress_{cat}_{canonical_user(who)}.txt"
    q = f"'{parent}' in parents and name = '{fname}' and trashed = false"
    resp = drive.files().list(q=q, fields="files(id,name)", pageSize=1,
                              supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    media = MediaIoBaseUpload(io.BytesIO(b"0"), mimetype="text/plain", resumable=False)
    meta = {"name": fname, "parents":[parent], "mimeType":"text/plain"}
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
        media = MediaIoBaseUpload(io.BytesIO(str(idx).encode()), mimetype="text/plain", resumable=False)
        drive.files().update(fileId=fid, media_body=media, supportsAllDrives=True).execute()
    except Exception:
        pass  # non-fatal

# ========================= UI state =========================
if "page" not in st.session_state: st.session_state.page = "home"
if "cat"  not in st.session_state: st.session_state.cat  = None
if "idx"  not in st.session_state: st.session_state.idx  = 0
if "dec"  not in st.session_state: st.session_state.dec  = {}   # pair_key -> {"hypo":..., "adv":...}
if "hq"   not in st.session_state: st.session_state.hq   = False
if "saving" not in st.session_state: st.session_state.saving = False
if "last_save_token" not in st.session_state: st.session_state.last_save_token = None

def go(p): st.session_state.page = p

# =========================== HOME ===========================
if st.session_state.page == "home":
    st.title("Image Triplet Filter")
    st.caption(f"Signed in as **{st.session_state.user}**")
    allowed = st.session_state.allowed
    cat_pick = st.selectbox("Select category", allowed)
    if st.button("Continue ➜", type="primary", key="home_go"):
        st.session_state.cat = cat_pick
        # always recompute from logs when entering dashboard
        st.session_state.dec = {}
        st.session_state.saving = False
        go("dashboard")

# ========================= DASHBOARD =========================
elif st.session_state.page == "dashboard":
    st.button("⬅️ Back", on_click=lambda: go("home"), key="dash_back")
    cat = st.session_state.cat
    if cat is None:
        st.warning("Pick a category first.")
        go("home"); st.stop()

    who = st.session_state.user
    cfg  = CAT[cat]
    meta = load_meta(cfg["jsonl_id"])

    # Build completion strictly from logs for this annotator (robust after crash/redeploy)
    completed_set, _, _ = build_completion_sets(cfg, who)

    total_pairs = len(meta)
    completed = sum(1 for e in meta if pk_of(e) in completed_set)
    pending   = total_pairs - completed

    st.subheader(f"Category: **{cat}**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total pairs", total_pairs)
    c2.metric("Completed (you)", completed)
    c3.metric("Pending (you)", pending)

    # Resume from first *undecided by logs*. Use old pointer only as a hint (max with it).
    hint_idx = load_progress_hint(cat, who)
    start_idx = max(hint_idx, first_undecided_index_for(meta, completed_set))

    if st.button("▶️ Start / Resume", type="primary", key="dash_start"):
        st.session_state.idx = start_idx
        st.session_state.saving = False
        go("review")

# ========================== REVIEW ==========================
elif st.session_state.page == "review":
    top_l, mid, top_r = st.columns([1,5,2])
    top_l.button("⬅️ Back", on_click=lambda: go("dashboard"), key="rev_back")
    st.session_state.hq = top_r.toggle("High quality images", value=st.session_state.hq,
                                       help="Toggle original bytes (slower) vs cached previews (faster)")

    who = st.session_state.user
    cat = st.session_state.cat
    cfg = CAT[cat]
    meta = load_meta(cfg["jsonl_id"])
    if not meta:
        st.warning("No records.")
        st.stop()

    # live progress from logs
    completed_set, log_h_map, log_a_map = build_completion_sets(cfg, who)
    st.caption(f"Progress: {len(completed_set)}/{len(meta)} completed for {who}")

    i = max(0, min(st.session_state.idx, len(meta)-1))
    entry = meta[i]
    hypo_name = entry.get("hypo_id", "")
    adv_name  = entry.get("adversarial_id", "")
    pk        = f"{hypo_name}|{adv_name}"

    # saved status for this annotator (from logs)
    saved_h = (log_h_map.get(pk, {}) or {}).get("status")
    saved_a = (log_a_map.get(pk, {}) or {}).get("status")

    # init temp session decisions from saved
    if pk not in st.session_state.dec:
        st.session_state.dec[pk] = {"hypo": saved_h, "adv": saved_a}

    st.subheader(f"{entry.get('id','(no id)')}  —  {pk}")
    with st.expander("📝 Text / Descriptions", expanded=True):
        st.markdown(f"**TEXT**: {entry.get('text','')}")
        st.markdown(f"**HYPOTHESIS (non-prototype)**: {entry.get('hypothesis','')}")
        st.markdown(f"**ADVERSARIAL (prototype)**: {entry.get('adversarial','')}")

    # resolve Drive IDs
    src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name)
    src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)

    c1, c2 = st.columns(2)

    # ---------- Hypothesis ----------
    with c1:
        st.markdown("**Hypothesis (non-proto)**")
        show_image(src_h_id, hypo_name, high_quality=st.session_state.hq)
        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Accept (hypo)", key=f"acc_h_{pk}", use_container_width=True):
                st.session_state.dec[pk]["hypo"] = "accepted"
        with b2:
            if st.button("❌ Reject (hypo)", key=f"rej_h_{pk}", use_container_width=True):
                st.session_state.dec[pk]["hypo"] = "rejected"
        st.caption(f"Current: {st.session_state.dec[pk]['hypo'] or '—'} | Saved: {saved_h or '—'}")

    # ---------- Adversarial ----------
    with c2:
        st.markdown("**Adversarial (proto)**")
        show_image(src_a_id, adv_name, high_quality=st.session_state.hq)
        b3, b4 = st.columns(2)
        with b3:
            if st.button("✅ Accept (adv)", key=f"acc_a_{pk}", use_container_width=True):
                st.session_state.dec[pk]["adv"] = "accepted"
        with b4:
            if st.button("❌ Reject (adv)", key=f"rej_a_{pk}", use_container_width=True):
                st.session_state.dec[pk]["adv"] = "rejected"
        st.caption(f"Current: {st.session_state.dec[pk]['adv'] or '—'} | Saved: {saved_a or '—'}")

    st.divider()

    # ---------- Save (idempotent & log-driven resume) ----------
    def save_now():
        st.session_state.saving = True
        dec = st.session_state.dec[pk]
        ts  = int(time.time())

        base = dict(entry)
        base["pair_key"]  = pk
        base["annotator"] = who                     # human-readable
        base["_annotator_canon"] = canonical_user(who)  # canonical (future-proof)

        rec_h = dict(base); rec_h.update({"side":"hypothesis", "status": dec.get("hypo") or "rejected", "decided_at": ts})
        rec_a = dict(base); rec_a.update({"side":"adversarial", "status": dec.get("adv") or "rejected", "decided_at": ts})

        # idempotence guard (per session)
        token = hashlib.sha1(json.dumps({"pk":pk, "h":rec_h["status"], "a":rec_a["status"], "who":canonical_user(who)}).encode()).hexdigest()
        if st.session_state.last_save_token == token:
            st.info("Already saved this exact decision.")
            st.session_state.saving = False
            return

        try:
            if rec_h["status"] == "accepted" and src_h_id:
                rec_h["copied_id"] = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg["dst_hypo"])
            if rec_a["status"] == "accepted" and src_a_id:
                rec_a["copied_id"] = create_shortcut_to_file(drive, src_a_id,  adv_name, cfg["dst_adv"])
        except HttpError as e:
            st.error(f"Drive action failed:\n{e}")
            st.session_state.saving = False
            return

        try:
            append_lines_to_drive_text(drive, cfg["log_hypo"], [json.dumps(rec_h, ensure_ascii=False) + "\n"])
            append_lines_to_drive_text(drive, cfg["log_adv"],  [json.dumps(rec_a, ensure_ascii=False) + "\n"])
        except HttpError as e:
            st.error(f"Failed to append logs: {e}")
            st.session_state.saving = False
            return

        # clear caches to reflect the fresh logs
        load_meta.clear()
        load_latest_map_for_annotator.clear()
        build_completion_sets.clear()

        st.session_state.last_save_token = token
        st.success("Saved.")
        st.session_state.saving = False

        # Jump to first undecided (log-driven). Store a pointer hint (optional).
        meta_local = load_meta(cfg["jsonl_id"])
        completed_set_local, _, _ = build_completion_sets(cfg, who)
        next_idx = first_undecided_index_for(meta_local, completed_set_local)
        st.session_state.idx = next_idx
        save_progress_hint(cat, who, next_idx)  # harmless hint
        st.rerun()

    nav_l, save_c, nav_r = st.columns([1,2,1])
    with nav_l:
        if st.button("⏮ Prev", key=f"prev_{pk}", use_container_width=True):
            st.session_state.idx = max(0, i-1)
            save_progress_hint(cat, st.session_state.user, st.session_state.idx)
            st.rerun()

    with save_c:
        st.button("💾 Save", type="primary", key=f"save_{pk}", use_container_width=True,
                  disabled=st.session_state.saving, on_click=save_now)

    with nav_r:
        if st.button("Next ⏭", key=f"next_{pk}", use_container_width=True):
            # move forward, but still persist a hint; dashboard will recompute from logs anyway
            st.session_state.idx = min(len(meta)-1, i+1)
            save_progress_hint(cat, st.session_state.user, st.session_state.idx)
            st.rerun()

import io, json, time
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ─────────────────────────── App config ───────────────────────────
st.set_page_config(page_title="Image Triplet Filter", layout="wide")

# ========== 1) Google Drive helpers (robust SA parsing) ==========
@st.cache_resource
def get_drive():
    """
    Return Drive service using service-account JSON from secrets.
    Tolerates both plain JSON and TOML-pasted strings with real newlines.
    """
    sa_raw = st.secrets["gcp"]["service_account"]
    if isinstance(sa_raw, str):
        # If private_key contains literal newlines, re-escape them
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
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()

def drive_upload_bytes(drive, parent_id: str, name: str, data: bytes,
                       mime="text/plain", file_id: str | None = None):
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    if file_id:
        return drive.files().update(fileId=file_id, media_body=media).execute()
    meta = {"name": name, "parents": [parent_id], "mimeType": mime}
    return drive.files().create(body=meta, media_body=media, fields="id,name").execute()

def find_file_id_in_folder(drive, folder_id: str, filename: str) -> str | None:
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def ensure_empty_txt_in_folder(drive, parent_id: str, name: str) -> str:
    fid = find_file_id_in_folder(drive, parent_id, name)
    if fid: return fid
    return drive_upload_bytes(drive, parent_id, name, b"", "text/plain")["id"]

def copy_file_to_folder(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
    body = {"name": new_name, "parents": [dest_folder_id]}
    return drive.files().copy(fileId=src_file_id, body=body, fields="id,name").execute()["id"]

def read_jsonl_from_drive(drive, file_id: str, max_lines: int | None = None):
    # Validate the ID first
    try:
        meta = drive.files().get(fileId=file_id, fields="id,name,mimeType,trashed").execute()
    except HttpError as e:
        st.error(
            "Could not access JSONL file.\n\n"
            "- Check the **file ID** (must be a file, not a folder).\n"
            "- Share the file with the **service account** (Viewer or Editor).\n\n"
            f"Drive API error: {e}"
        )
        st.stop()

    if meta.get("trashed"):
        st.error(f"The JSONL file `{meta.get('name')}` is in Trash. Restore it first.")
        st.stop()

    if meta.get("mimeType") == "application/vnd.google-apps.folder":
        st.error(
            f"The provided ID is a **folder**, not a file: `{meta.get('name')}`.\n"
            "Please paste the **file** ID of your JSONL into secrets."
        )
        st.stop()

    # Download the bytes
    try:
        raw = drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")
    except HttpError as e:
        st.error(
            "Failed to download JSONL from Drive.\n\n"
            "Common causes:\n"
            "- The service account does **not** have permission on the file.\n"
            "- The file is a Google Doc/Sheet (needs export), not a plain file.\n\n"
            f"Drive API error: {e}"
        )
        st.stop()

    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
        if max_lines and len(out) >= max_lines:
            break
    return out

def read_text_from_drive(drive, file_id: str) -> str:
    return drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")

def append_lines_to_drive_text(drive, file_id: str, new_lines: list[str]):
    prev = read_text_from_drive(drive, file_id)
    updated = prev + "".join(new_lines)
    drive_upload_bytes(drive, parent_id="", name="", data=updated.encode("utf-8"), file_id=file_id)

# ========== 2) Category configuration from secrets ==========
# Expect these keys inside [gcp] in secrets.toml
CAT_CFG = {
    "demography": {
        "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["demography_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],   # accepted only
        "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],    # accepted only
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

# ───────── helpers bound to category ─────────
@st.cache_data(show_spinner=False)
def load_meta(jsonl_id: str):
    # Load ALL lines once for the active category
    return read_jsonl_from_drive(drive, jsonl_id, max_lines=None)

@st.cache_data(show_spinner=False)
def load_decisions_map(file_id: str):
    """
    Return a dict keyed by image filename, holding the last decision record.
    Each line in the log is one JSON object { ... , "status": "accepted"/"rejected", "side": "hypo"/"adv", ... }
    """
    try:
        txt = read_text_from_drive(drive, file_id)
    except Exception:
        return {}
    decided = {}
    for line in txt.splitlines():
        line = line.strip()
        if not line: continue
        try:
            rec = json.loads(line)
            key = rec.get("image_name") or rec.get("image_id") or rec.get("hypo_id") or rec.get("adversarial_id")
            if key:
                decided[key] = rec
        except Exception:
            continue
    return decided

def first_undecided_index(meta, d_h, d_a):
    """
    Find first index where either hypo or adv image has no decision yet.
    """
    for i, m in enumerate(meta):
        h_name = m.get("hypo_id")
        a_name = m.get("adversarial_id")
        if (h_name and h_name not in d_h) or (a_name and a_name not in d_a):
            return i
    return 0

# ========== 3) Simple router: home → dashboard → review ==========
if "page" not in st.session_state:
    st.session_state.page = "home"
if "cat" not in st.session_state:
    st.session_state.cat = None
if "idx" not in st.session_state:
    st.session_state.idx = 0  # current record index within selected category

def go(page): st.session_state.page = page

# ─────────────────────────── HOME ───────────────────────────
if st.session_state.page == "home":
    st.markdown("## Image Triplet Filter")

    cat = st.selectbox("Select category", list(CAT_CFG.keys()))
    if st.button("Continue ➜", type="primary"):
        st.session_state.cat = cat
        st.session_state.idx = 0
        go("dashboard")

# ──────────────────────── DASHBOARD ─────────────────────────
elif st.session_state.page == "dashboard":
    st.button("⬅️ Back", on_click=lambda: go("home"))
    cat = st.session_state.cat
    if cat is None:
        st.warning("Pick a category first.")
        go("home")
        st.stop()

    cfg = CAT_CFG[cat]
    meta = load_meta(cfg["jsonl_id"])
    dec_h = load_decisions_map(cfg["log_hypo"])
    dec_a = load_decisions_map(cfg["log_adv"])

    total = len(meta)
    hypo_done = sum(1 for m in meta if m.get("hypo_id") in dec_h)
    adv_done  = sum(1 for m in meta if m.get("adversarial_id") in dec_a)
    completed = sum(1 for m in meta if (m.get("hypo_id") in dec_h) and (m.get("adversarial_id") in dec_a))
    pending = total - completed

    st.markdown(f"### Category: **{cat}**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total records", total)
    c2.metric("Completed (both decided)", completed)
    c3.metric("Hypothesis decided", hypo_done)
    c4.metric("Adversarial decided", adv_done)
    st.info(f"Pending to fully complete: **{pending}**")

    if st.button("▶️ Start / Resume", type="primary"):
        # jump to first undecided
        st.session_state.idx = first_undecided_index(meta, dec_h, dec_a)
        go("review")

# ───────────────────────── REVIEW ───────────────────────────
elif st.session_state.page == "review":
    top_left = st.columns([1, 8])[0]
    top_left.button("⬅️ Back", on_click=lambda: go("dashboard"))

    cat = st.session_state.cat
    cfg = CAT_CFG[cat]

    # data
    meta = load_meta(cfg["jsonl_id"])
    if not meta:
        st.warning("No records.")
        st.stop()

    # clamp idx
    st.session_state.idx = max(0, min(st.session_state.idx, len(meta)-1))
    i = st.session_state.idx
    entry = meta[i]

    # decisions (per-side)
    dec_h = load_decisions_map(cfg["log_hypo"])
    dec_a = load_decisions_map(cfg["log_adv"])

    # text blocks
    st.markdown(f"### {entry.get('id','(no id)')}")
    with st.expander("📝 TEXT & Descriptions", expanded=True):
        st.markdown(f"**TEXT**: {entry.get('text','')}")
        st.markdown(f"**HYPOTHESIS (non-prototype)**: {entry.get('hypothesis','')}")
        st.markdown(f"**ADVERSARIAL (prototype)**: {entry.get('adversarial','')}")

    # image names
    hypo_name = entry.get("hypo_id")
    adv_name  = entry.get("adversarial_id")

    # resolve source drive file ids
    src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name) if hypo_name else None
    src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)  if adv_name  else None

    # Layout: two columns with per-image controls
    c1, c2 = st.columns(2)

    # ---------- HYPOTHESIS SIDE ----------
    with c1:
        st.markdown("**Hypothesis (non-proto)**")
        if src_h_id:
            st.image(drive_download_bytes(drive, src_h_id), caption=hypo_name, use_column_width=True)
        else:
            st.error(f"Missing image: {hypo_name}")

        # Decision state & buttons
        decided_h = dec_h.get(hypo_name)
        if decided_h:
            st.success(f"Status: **{decided_h.get('status','?')}**")
        b1, b2 = st.columns(2)

        def log_h(status: str, saved_id=None):
            rec = dict(entry)
            rec.update({
                "side": "hypo",
                "status": status,           # accepted / rejected
                "image_name": hypo_name,
                "src_file_id": src_h_id,
                "copied_file_id": saved_id,
                "decided_at": int(time.time()),
            })
            append_lines_to_drive_text(drive, cfg["log_hypo"], [json.dumps(rec, ensure_ascii=False) + "\n"])
            load_decisions_map.clear()

        with b1:
            ok = st.button("✅ Accept (copy)", use_container_width=True, disabled=(not src_h_id) or bool(decided_h))
            if ok:
                new_id = copy_file_to_folder(drive, src_h_id, hypo_name, cfg["dst_hypo"]) if src_h_id else None
                log_h("accepted", new_id)
                st.rerun()

        with b2:
            ok = st.button("❌ Reject (log)", use_container_width=True, disabled=bool(decided_h))
            if ok:
                log_h("rejected", None)
                st.rerun()

    # ---------- ADVERSARIAL SIDE ----------
    with c2:
        st.markdown("**Adversarial (proto)**")
        if src_a_id:
            st.image(drive_download_bytes(drive, src_a_id), caption=adv_name, use_column_width=True)
        else:
            st.error(f"Missing image: {adv_name}")

        decided_a = dec_a.get(adv_name)
        if decided_a:
            st.success(f"Status: **{decided_a.get('status','?')}**")
        c21, c22 = st.columns(2)

        def log_a(status: str, saved_id=None):
            rec = dict(entry)
            rec.update({
                "side": "adv",
                "status": status,
                "image_name": adv_name,
                "src_file_id": src_a_id,
                "copied_file_id": saved_id,
                "decided_at": int(time.time()),
            })
            append_lines_to_drive_text(drive, cfg["log_adv"], [json.dumps(rec, ensure_ascii=False) + "\n"])
            load_decisions_map.clear()

        with c21:
            ok = st.button("✅ Accept (copy)", use_container_width=True, disabled=(not src_a_id) or bool(decided_a))
            if ok:
                new_id = copy_file_to_folder(drive, src_a_id, adv_name, cfg["dst_adv"]) if src_a_id else None
                log_a("accepted", new_id)
                st.rerun()

        with c22:
            ok = st.button("❌ Reject (log)", use_container_width=True, disabled=bool(decided_a))
            if ok:
                log_a("rejected", None)
                st.rerun()

    # ---------- Prev / Next navigation (bottom) ----------
    nav_left, nav_right = st.columns([1,1])
    with nav_left:
        if st.button("⏮ Prev"):
            st.session_state.idx = max(0, i - 1)
            st.rerun()
    with nav_right:
        if st.button("Next ⏭"):
            # Prefer jump to next UNDONE record
            next_i = i + 1
            # refresh maps for an up-to-date check
            dec_h2 = load_decisions_map(cfg["log_hypo"])
            dec_a2 = load_decisions_map(cfg["log_adv"])
            while next_i < len(meta):
                nm = meta[next_i]
                hname = nm.get("hypo_id")
                aname = nm.get("adversarial_id")
                if (hname and hname not in dec_h2) or (aname and aname not in dec_a2):
                    break
                next_i += 1
            st.session_state.idx = min(next_i, len(meta)-1)
            st.rerun()
            

#===============================================================================================================================================#
# import io, json, time
# import streamlit as st

# # Google Drive API
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# # ─────────────────────────── App config ───────────────────────────
# st.set_page_config(page_title="Image Triplet Filter", layout="wide")

# # ========== 1) Google Drive helpers (robust SA parsing) ==========
# @st.cache_resource
# def get_drive():
#     """
#     Return Drive service using service-account JSON from secrets.
#     Tolerates both plain JSON and '{...}' style with real newlines.
#     """
#     sa_raw = st.secrets["gcp"]["service_account"]
#     if isinstance(sa_raw, str):
#         # If private_key contains literal newlines, re-escape them
#         if '"private_key"' in sa_raw and "\n" in sa_raw and "\\n" not in sa_raw:
#             sa_raw = sa_raw.replace("\r\n", "\\n").replace("\n", "\\n")
#         sa = json.loads(sa_raw)
#     else:
#         sa = dict(sa_raw)

#     creds = service_account.Credentials.from_service_account_info(
#         sa, scopes=["https://www.googleapis.com/auth/drive"]
#     )
#     return build("drive", "v3", credentials=creds)

# def drive_download_bytes(drive, file_id: str) -> bytes:
#     req = drive.files().get_media(fileId=file_id)
#     buf = io.BytesIO()
#     dl = MediaIoBaseDownload(buf, req)
#     done = False
#     while not done:
#         _, done = dl.next_chunk()
#     buf.seek(0)
#     return buf.read()

# def drive_upload_bytes(drive, parent_id: str, name: str, data: bytes,
#                        mime="text/plain", file_id: str | None = None):
#     media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
#     if file_id:
#         return drive.files().update(fileId=file_id, media_body=media).execute()
#     meta = {"name": name, "parents": [parent_id], "mimeType": mime}
#     return drive.files().create(body=meta, media_body=media, fields="id,name").execute()

# def find_file_id_in_folder(drive, folder_id: str, filename: str) -> str | None:
#     q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
#     resp = drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
#     files = resp.get("files", [])
#     return files[0]["id"] if files else None

# def ensure_empty_txt_in_folder(drive, parent_id: str, name: str) -> str:
#     fid = find_file_id_in_folder(drive, parent_id, name)
#     if fid: return fid
#     return drive_upload_bytes(drive, parent_id, name, b"", "text/plain")["id"]

# def copy_file_to_folder(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
#     body = {"name": new_name, "parents": [dest_folder_id]}
#     return drive.files().copy(fileId=src_file_id, body=body, fields="id,name").execute()["id"]

# def read_jsonl_from_drive(drive, file_id: str, max_lines: int | None = None):
#     # Validate the ID first: make sure it’s a real file, not a folder,
#     # and that we have permission.
#     try:
#         meta = drive.files().get(fileId=file_id, fields="id,name,mimeType,trashed").execute()
#     except HttpError as e:
#         st.error(
#             "Could not access JSONL file.\n\n"
#             f"- Check that the **ID** is correct (file, not folder).\n"
#             f"- Share the file with the **service account** email (Viewer or Editor).\n\n"
#             f"Drive API error: {e}"
#         )
#         st.stop()

#     if meta.get("trashed"):
#         st.error(f"The JSONL file `{meta.get('name')}` is in Trash. Restore it first.")
#         st.stop()

#     if meta.get("mimeType") == "application/vnd.google-apps.folder":
#         st.error(
#             f"The provided ID is a **folder**, not a file: `{meta.get('name')}`.\n"
#             "Please paste the **file** ID of your JSONL into secrets."
#         )
#         st.stop()

#     # Now download the bytes
#     try:
#         raw = drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")
#     except HttpError as e:
#         st.error(
#             "Failed to download JSONL from Drive.\n\n"
#             "Common causes:\n"
#             "- The service account does **not** have permission on the file.\n"
#             "- The file is a Google Doc/Sheet (needs export), not a plain file.\n\n"
#             f"Drive API error: {e}"
#         )
#         st.stop()

#     out = []
#     for line in raw.splitlines():
#         line = line.strip()
#         if not line:
#             continue
#         try:
#             out.append(json.loads(line))
#         except Exception:
#             continue
#         if max_lines and len(out) >= max_lines:
#             break
#     return out

# def read_text_from_drive(drive, file_id: str) -> str:
#     return drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")

# def append_lines_to_drive_text(drive, file_id: str, new_lines: list[str]):
#     prev = read_text_from_drive(drive, file_id)
#     updated = prev + "".join(new_lines)
#     drive_upload_bytes(drive, parent_id="", name="", data=updated.encode("utf-8"), file_id=file_id)

# # ========== 2) Category configuration from secrets ==========
# # Expect these keys inside [gcp] in secrets.toml
# CAT_CFG = CATEGORY_CFG = {
#     "demography": {
#         "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],

#         # --- Source (unfiltered) image folders ---
#         "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
#         "src_adv":  st.secrets["gcp"]["demography_adv_folder"],

#         # --- Destination (filtered, accepted-only) folders ---
#         "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],
#         "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],

#         # --- Log file IDs (Drive JSONL for filtered results) ---
#         "log_hypo": st.secrets["gcp"]["demography_hypo_filtered_log_id"],
#         "log_adv":  st.secrets["gcp"]["demography_adv_filtered_log_id"],

#         # --- Filename prefixes ---
#         "hypo_prefix": "dem_h",
#         "adv_prefix":  "dem_ah",
#     },

#     "animal": {
#         "jsonl_id": st.secrets["gcp"]["animal_jsonl_id"],
        
#         # --- Source (unfiltered) image folders ---
#         "src_hypo": st.secrets["gcp"]["animal_hypo_folder"],
#         "src_adv":  st.secrets["gcp"]["animal_adv_folder"],

#         # --- Destination (filtered, accepted-only) folders ---
#         "dst_hypo": st.secrets["gcp"]["animal_hypo_filtered"],
#         "dst_adv":  st.secrets["gcp"]["animal_adv_filtered"],

#         # --- Log file IDs (Drive JSONL for filtered results) ---
#         "log_hypo": st.secrets["gcp"]["animal_hypo_filtered_log_id"],
#         "log_adv":  st.secrets["gcp"]["animal_adv_filtered_log_id"],

#         # --- Filename prefixes ---
#         "hypo_prefix": "ani_h",
#         "adv_prefix":  "ani_ah",
#     },

#     "objects": {
#         "jsonl_id": st.secrets["gcp"]["objects_jsonl_id"],

#         # --- Source (unfiltered) image folders ---
#         "src_hypo": st.secrets["gcp"]["objects_hypo_folder"],
#         "src_adv":  st.secrets["gcp"]["objects_adv_folder"],

#         # --- Destination (filtered, accepted-only) folders ---
#         "dst_hypo": st.secrets["gcp"]["objects_hypo_filtered"],
#         "dst_adv":  st.secrets["gcp"]["objects_adv_filtered"],

#         # --- Log file IDs (Drive JSONL for filtered results) ---
#         "log_hypo": st.secrets["gcp"]["objects_hypo_filtered_log_id"],
#         "log_adv":  st.secrets["gcp"]["objects_adv_filtered_log_id"],

#         # --- Filename prefixes ---
#         "hypo_prefix": "obj_h",
#         "adv_prefix":  "obj_ah",
#     },
# }

# drive = get_drive()

# # ───────── helpers bound to category ─────────
# @st.cache_data(show_spinner=False)
# def load_meta(jsonl_id: str):
#     # Load ALL lines once for the active category
#     return read_jsonl_from_drive(drive, jsonl_id, max_lines=None)

# @st.cache_data(show_spinner=False)
# def load_decisions_map(log_file_id: str):
#     txt = read_text_from_drive(drive, log_file_id)
#     decided = {}
#     for line in txt.splitlines():
#         line = line.strip()
#         if not line: continue
#         try:
#             rec = json.loads(line)
#             decided[rec["id"]] = rec
#         except Exception:
#             continue
#     return decided

# def get_or_create_log_id(cat_key: str, cfg: dict):
#     g = st.secrets["gcp"]
#     per_cat_log_id = CAT_CFG[cat_key]["filtered_log_id"](g)
#     if per_cat_log_id:  # use provided file
#         return per_cat_log_id
#     # else create under parent folder
#     parent = g.get("filtered_logs_parent")
#     if not parent:
#         st.stop()  # explicit missing config
#     return ensure_empty_txt_in_folder(drive, parent, CAT_CFG[cat_key]["log_name"])

# # ========== 3) Simple router: home → dashboard → review ==========
# if "page" not in st.session_state:
#     st.session_state.page = "home"
# if "cat" not in st.session_state:
#     st.session_state.cat = None
# if "idx" not in st.session_state:
#     st.session_state.idx = 0  # current record index within selected category

# def go(page): st.session_state.page = page

# # ─────────────────────────── HOME ───────────────────────────
# if st.session_state.page == "home":
#     st.markdown("## Image Triplet Filter")

#     # Category chooser
#     cat = st.selectbox("Select category", list(CAT_CFG.keys()))
#     if st.button("Continue ➜", type="primary"):
#         st.session_state.cat = cat
#         # reset index when switching category
#         st.session_state.idx = 0
#         go("dashboard")

# # ──────────────────────── DASHBOARD ─────────────────────────
# elif st.session_state.page == "dashboard":
#     st.button("⬅️ Back", on_click=lambda: go("home"))
#     cat = st.session_state.cat
#     if cat is None:
#         st.warning("Pick a category first.")
#         go("home")
#         st.stop()

#     g = st.secrets["gcp"]
#     try:
#         # gather IDs
#         jsonl_id = CAT_CFG[cat]["jsonl_id"](g)
#         log_id   = get_or_create_log_id(cat, CAT_CFG[cat])
#     except KeyError as e:
#         st.error(f"Missing secrets key: {e}")
#         st.stop()

#     # load metadata + decisions
#     meta = load_meta(jsonl_id)
#     decided_map = load_decisions_map(log_id)

#     total = len(meta)
#     completed = sum(1 for m in meta if m["id"] in decided_map)
#     pending = total - completed

#     st.markdown(f"### Category: **{cat}**")
#     c1, c2, c3 = st.columns(3)
#     c1.metric("Total records", total)
#     c2.metric("Completed", completed)
#     c3.metric("Pending", pending)

#     if st.button("▶️ Start / Resume", type="primary"):
#         # move pointer to first undecided if current landed on a decided one
#         # else keep where it is (resume)
#         if 0 <= st.session_state.idx < total and meta[st.session_state.idx]["id"] not in decided_map:
#             pass
#         else:
#             # find first undecided
#             nxt = next((i for i, m in enumerate(meta) if m["id"] not in decided_map), 0)
#             st.session_state.idx = nxt
#         go("review")

# # ───────────────────────── REVIEW ───────────────────────────
# elif st.session_state.page == "review":
#     # Top bar
#     left = st.columns([1, 5])[0]
#     left.button("⬅️ Back", on_click=lambda: go("dashboard"))

#     cat = st.session_state.cat
#     g = st.secrets["gcp"]
#     try:
#         jsonl_id = CAT_CFG[cat]["jsonl_id"](g)
#         src_hypo = CAT_CFG[cat]["src_hypo_folder"](g)
#         src_adv  = CAT_CFG[cat]["src_adv_folder"](g)
#         dst_hypo = CAT_CFG[cat]["dst_hypo_folder"](g)
#         dst_adv  = CAT_CFG[cat]["dst_adv_folder"](g)
#         log_id   = get_or_create_log_id(cat, CAT_CFG[cat])
#     except KeyError as e:
#         st.error(f"Missing secrets key: {e}")
#         st.stop()

#     # data
#     meta = load_meta(jsonl_id)
#     if not meta:
#         st.warning("No records.")
#         st.stop()

#     # clamp idx
#     st.session_state.idx = max(0, min(st.session_state.idx, len(meta)-1))
#     i = st.session_state.idx
#     entry = meta[i]

#     # decisions
#     decided_map = load_decisions_map(log_id)
#     status_badge = ""
#     if entry["id"] in decided_map:
#         status_badge = f" &nbsp;&nbsp;✅ Already **{decided_map[entry['id']]['status']}**"
#     st.markdown(f"### {entry.get('id','(no id)')}{status_badge}")

#     # text blocks
#     with st.expander("📝 Text / Descriptions", expanded=True):
#         st.markdown(f"**TEXT**: {entry.get('text','')}")
#         st.markdown(f"**HYPOTHESIS (non-prototype)**: {entry.get('hypothesis','')}")
#         st.markdown(f"**ADVERSARIAL (prototype)**: {entry.get('adversarial','')}")

#     # image names
#     hypo_name = entry.get("hypo_id")
#     adv_name  = entry.get("adversarial_id")

#     # resolve source drive file ids
#     src_h_id = find_file_id_in_folder(drive, src_hypo, hypo_name) if hypo_name else None
#     src_a_id = find_file_id_in_folder(drive, src_adv,  adv_name)  if adv_name  else None

#     # layout: two images side by side
#     c1, c2 = st.columns(2)
#     with c1:
#         st.markdown("**Hypothesis (non-proto)**")
#         if src_h_id:
#             st.image(drive_download_bytes(drive, src_h_id), caption=hypo_name, use_column_width=True)
#         else:
#             st.error(f"Missing image: {hypo_name}")

#         # Accept/Reject for the pair lives below, not per-image.

#     with c2:
#         st.markdown("**Adversarial (proto)**")
#         if src_a_id:
#             st.image(drive_download_bytes(drive, src_a_id), caption=adv_name, use_column_width=True)
#         else:
#             st.error(f"Missing image: {adv_name}")

#     # Accept / Reject row (single decision for the pair)
#     a1, a2, a3 = st.columns([1,1,3])

#     def write_decision(status: str, copied_h=None, copied_a=None):
#         rec = dict(entry)
#         rec.update({
#             "status": status,
#             "hypo_copied_id": copied_h,
#             "adv_copied_id":  copied_a,
#             "decided_at": int(time.time())
#         })
#         append_lines_to_drive_text(drive, log_id, [json.dumps(rec, ensure_ascii=False) + "\n"])
#         # refresh cache & move forward
#         load_decisions_map.clear()
#         st.session_state.idx = min(i + 1, len(meta)-1)
#         st.rerun()

#     with a1:
#         ok = st.button("✅ Accept", type="primary", use_container_width=True,
#                        disabled=not (src_h_id and src_a_id))
#         if ok:
#             new_h_id = copy_file_to_folder(drive, src_h_id, hypo_name, dst_hypo) if src_h_id else None
#             new_a_id = copy_file_to_folder(drive, src_a_id,  adv_name,  dst_adv)  if src_a_id else None
#             write_decision("selected", new_h_id, new_a_id)

#     with a2:
#         if st.button("❌ Reject", use_container_width=True):
#             write_decision("rejected")

#     # Prev / Next navigation at the bottom
#     n1, n2, n3 = st.columns([1,1,3])
#     with n1:
#         if st.button("⏮ Prev"):
#             st.session_state.idx = max(0, i - 1)
#             st.rerun()
#     with n2:
#         if st.button("Next ⏭"):
#             st.session_state.idx = min(len(meta) - 1, i + 1)
#             st.rerun()

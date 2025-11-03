import io, json, time
from typing import Dict, List, Optional
import streamlit as st
from PIL import Image

# ---------- AUTH & DRIVE HELPERS ----------
from google.oauth2.service_account import Credentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

@st.cache_resource(show_spinner=False)
def get_drive() -> GoogleDrive:
    sa_info = dict(st.secrets["gcp_service_account"])  # service account JSON from secrets
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gauth = GoogleAuth(settings={
        "client_config_backend": "service",
        "service_config": {
            "client_user_email": sa_info["client_email"],
            "client_json": sa_info,
        },
        "save_credentials": False,
        "get_refresh_token": False,
        "oauth_scope": " ".join(SCOPES),
    })
    gauth.credentials = creds
    return GoogleDrive(gauth)

def drive_list_children(drive: GoogleDrive, parent_id: str, name_eq: Optional[str] = None, mime_type: Optional[str] = None) -> List[dict]:
    q = f"'{parent_id}' in parents and trashed=false"
    if name_eq:
        q += f" and name='{name_eq}'"
    if mime_type:
        q += f" and mimeType='{mime_type}'"
    files = drive.ListFile({'q': q, 'fields': 'files(id, name, mimeType, parents)'}).GetList()
    return files

def drive_ensure_folder(drive: GoogleDrive, parent_id: str, folder_name: str) -> str:
    exist = drive_list_children(drive, parent_id, name_eq=folder_name, mime_type="application/vnd.google-apps.folder")
    if exist:
        return exist[0]['id']
    f = drive.CreateFile({'title': folder_name, 'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [{'id': parent_id}]})
    f.Upload()
    return f['id']

def drive_read_text(drive: GoogleDrive, file_id: str) -> str:
    f = drive.CreateFile({'id': file_id})
    return f.GetContentString()

def drive_write_text(drive: GoogleDrive, parent_id: str, name: str, text: str, file_id: Optional[str] = None) -> str:
    if file_id:
        f = drive.CreateFile({'id': file_id})
    else:
        f = drive.CreateFile({'title': name, 'name': name, 'parents': [{'id': parent_id}]})
    f.SetContentString(text, encoding='utf-8')
    f.Upload()
    return f['id']

def drive_find_file(drive: GoogleDrive, parent_id: str, name: str) -> Optional[str]:
    hit = drive_list_children(drive, parent_id, name_eq=name)
    return hit[0]['id'] if hit else None

def drive_download_image(drive: GoogleDrive, parent_id: str, filename: str) -> Optional[Image.Image]:
    file_id = drive_find_file(drive, parent_id, filename)
    if not file_id:
        return None
    f = drive.CreateFile({'id': file_id})
    buf = io.BytesIO()
    f.GetContentFile('/tmp/_tmp_img', mimetype='image/png')  # fallback path
    # re-open to PIL
    try:
        img = Image.open('/tmp/_tmp_img').convert("RGB")
        return img
    except Exception:
        # alternate: read bytes directly
        b = io.BytesIO(f.GetContentBinary())
        try:
            return Image.open(b).convert("RGB")
        except Exception:
            return None

def drive_copy_to_folder(drive: GoogleDrive, src_parent_id: str, filename: str, dst_parent_id: str) -> Optional[str]:
    src_id = drive_find_file(drive, src_parent_id, filename)
    if not src_id:
        return None
    src = drive.CreateFile({'id': src_id})
    copied = drive.CreateFile({'title': filename, 'parents': [{'id': dst_parent_id}]})
    copied.SetContentFile('/tmp/_tmp_copy')  # we must download+reupload for service accounts reliably
    # download then upload
    src.GetContentFile('/tmp/_tmp_copy')
    copied.Upload()
    return copied['id']

# ---------- SIMPLE LOGIN ----------
# def require_login():
#     if "auth_user" not in st.session_state:
#         st.session_state.auth_user = None

#     if st.session_state.auth_user:
#         return True

#     st.title("Image Filtering Login")
#     user = st.text_input("Name", placeholder="e.g., Gagan")
#     pwd  = st.text_input("Password", type="password", placeholder="Provided password")
#     if st.button("Sign in"):
#         users = st.secrets.get("users", {})
#         if user in users and str(users[user]) == str(pwd):
#             st.session_state.auth_user = user
#             st.experimental_rerun()
#         else:
#             st.error("Invalid credentials.")
#     st.stop()
def require_login():
    if "auth_user" not in st.session_state:
        st.session_state.auth_user = None

    if st.session_state.auth_user:
        return True

    st.title("Image Filtering Login")
    user_in = st.text_input("Name", placeholder="e.g., Gagan")
    pwd_in  = st.text_input("Password", type="password", placeholder="Provided password")

    if st.button("Sign in"):
        users_map = st.secrets.get("users", {})
        # normalize: case-insensitive username, strip spaces
        norm = {k.strip().casefold(): str(v) for k, v in users_map.items()}
        if user_in.strip().casefold() in norm and str(pwd_in) == norm[user_in.strip().casefold()]:
            st.session_state.auth_user = user_in.strip()
            st.experimental_rerun()
        else:
            st.error("Invalid credentials.")
    st.stop()
# ---------- APP ----------
def main():
    require_login()
    user = st.session_state.auth_user
    st.sidebar.success(f"Logged in as: {user}")

    drive = get_drive()

    st.sidebar.header("Drive config (one-time)")
    base_folder_id = st.sidebar.text_input(
        "Dataset images BASE folder ID",
        help="The Google Drive folder ID of: dataset/images",
        placeholder="e.g., 1AbCDeFg... (folder id)"
    )

    if not base_folder_id:
        st.info("Paste your `dataset/images` folder ID in the sidebar to continue.")
        st.stop()

    # discover category subfolders & JSONLs by name
    # Required structure:
    # dataset/images/
    #   demography/hypo, demography/adv_hypo
    #   animal/hypo, animal/adv_hypo
    #   objects/hypo, objects/adv_hypo
    # and JSONL files in dataset/images parent: demography_images.jsonl, animal_images.jsonl, objects_images.jsonl

    # Find category folders
    def find_category(root_id: str, cat: str):
        cat_id = drive_ensure_folder(drive, root_id, cat)
        hypo_id = drive_ensure_folder(drive, cat_id, "hypo")
        adv_id  = drive_ensure_folder(drive, cat_id, "adv_hypo")
        # filtered subfolders under category
        filtered_id = drive_ensure_folder(drive, cat_id, "filtered")
        f_hypo = drive_ensure_folder(drive, filtered_id, "hypo")
        f_adv  = drive_ensure_folder(drive, filtered_id, "adv_hypo")
        return dict(cat=cat_id, hypo=hypo_id, adv=adv_id, filt=filtered_id, filt_hypo=f_hypo, filt_adv=f_adv)

    cats = {
        "demography": find_category(base_folder_id, "demography"),
        "animal":     find_category(base_folder_id, "animal"),
        "objects":    find_category(base_folder_id, "objects"),
    }

    # Find JSONL files in BASE (parent of category folders)
    # These are one level up (the same base folder)
    def find_jsonl(root_id: str, name: str) -> Optional[str]:
        return drive_find_file(drive, root_id, name)

    jsonl_ids = {
        "demography": find_jsonl(base_folder_id, "demography_images.jsonl"),
        "animal":     find_jsonl(base_folder_id, "animal_images.jsonl"),
        "objects":    find_jsonl(base_folder_id, "objects_images.jsonl"),
    }

    missing = [k for k,v in jsonl_ids.items() if v is None]
    if missing:
        st.warning(f"Missing JSONL in base folder for: {', '.join(missing)}. "
                   f"Expected files: demography_images.jsonl, animal_images.jsonl, objects_images.jsonl")

    cat_choice = st.sidebar.radio("Category", ["demography", "animal", "objects"], horizontal=True)
    st.title(f"Filtering · {cat_choice}")

    # Load JSONL (cached per file id)
    @st.cache_data(show_spinner=False)
    def load_jsonl(fid: str) -> List[dict]:
        raw = drive_read_text(drive, fid)
        rows = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
        return rows

    file_id = jsonl_ids.get(cat_choice)
    if not file_id:
        st.stop()

    data = load_jsonl(file_id)
    total = len(data)
    if total == 0:
        st.info("No records found in JSONL.")
        st.stop()

    # Progress file per user+category
    progress_name = f"progress_{user}_{cat_choice}.json"
    prog_id = drive_find_file(drive, base_folder_id, progress_name)
    if prog_id:
        try:
            prog = json.loads(drive_read_text(drive, prog_id))
        except Exception:
            prog = {"idx": 0}
    else:
        prog = {"idx": 0}

    idx = st.session_state.get("idx", prog.get("idx", 0))
    idx = max(0, min(idx, total-1))

    # Current record
    row = data[idx]

    # Resolve folders for images
    folders = cats[cat_choice]
    hypo_folder = folders["hypo"]
    adv_folder  = folders["adv"]
    filt_hypo   = folders["filt_hypo"]
    filt_adv    = folders["filt_adv"]

    # Display metadata
    with st.expander("Metadata", expanded=True):
        left, right = st.columns(2)
        with left:
            st.markdown(f"**ID**: `{row.get('id','')}`")
            st.markdown(f"**Knob**: `{row.get('knob','')}` · **Attr**: `{row.get('attr_token','')}`")
            st.markdown(f"**Category fields**: `{row.get('group_category','')}` / `{row.get('socio_attr','')}` / `{row.get('pole','')}`")
        with right:
            st.code(row.get("text",""), language=None)
            st.code(row.get("hypothesis",""), language=None)
            st.code(row.get("adversarial",""), language=None)

    # Load images
    hyp_name = row.get("hypo_id")
    adv_name = row.get("adversarial_id")
    hyp_img = drive_download_image(drive, hypo_folder, hyp_name) if hyp_name else None
    adv_img = drive_download_image(drive, adv_folder,  adv_name) if adv_name else None

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Hypothesis (should be TRUE)")
        if hyp_img: st.image(hyp_img, use_column_width=True)
        else: st.error(f"Image not found: {hyp_name}")
    with c2:
        st.subheader("Adversarial (should be SLIGHTLY WRONG)")
        if adv_img: st.image(adv_img, use_column_width=True)
        else: st.error(f"Image not found: {adv_name}")

    st.caption(f"{idx+1} / {total}")

    colA, colB, colC, colD = st.columns([1,1,1,2])
    if colA.button("⬅️ Back", disabled=(idx==0)):
        idx = max(0, idx-1)
        st.session_state.idx = idx
        st.rerun()

    approved = colB.button("✅ Approve")
    rejected = colC.button("🚫 Reject")

    if approved or rejected:
        # on approve: copy images to filtered/{category}/hypo|adv_hypo and append to filtered jsonl
        if approved and hyp_name and adv_name:
            drive_copy_to_folder(drive, hypo_folder, hyp_name, filt_hypo)
            drive_copy_to_folder(drive,  adv_folder,  adv_name,  filt_adv)

            # append to filtered jsonl in base
            filt_name = f"{cat_choice}_filtered.jsonl"
            filt_id = drive_find_file(drive, base_folder_id, filt_name)
            line = json.dumps(row, ensure_ascii=False)
            if filt_id:
                # read-append-write (simple & safe for small/med files)
                existing = drive_read_text(drive, filt_id) + ("\n" if not existing_endswith_newline(existing:=drive_read_text(drive, filt_id)) else "")
                drive_write_text(drive, base_folder_id, filt_name, existing + line + "\n", file_id=filt_id)
            else:
                drive_write_text(drive, base_folder_id, filt_name, line + "\n", file_id=None)

        # advance index
        idx = min(total-1, idx+1)
        st.session_state.idx = idx
        # persist progress
        prog["idx"] = idx
        if prog_id:
            drive_write_text(drive, base_folder_id, progress_name, json.dumps(prog), file_id=prog_id)
        else:
            prog_id = drive_write_text(drive, base_folder_id, progress_name, json.dumps(prog), file_id=None)
        st.rerun()

def existing_endswith_newline(s: str) -> bool:
    return len(s) > 0 and s[-1] == "\n"

import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build

st.set_page_config(page_title="Image Filtering", layout="wide")
st.set_option('client.showErrorDetails', True)

@st.cache_resource(show_spinner=False)
def get_drive_client(creds_dict):
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

@st.cache_data(show_spinner=False)
def list_folder_files(drive, folder_id):
    # lightweight listing; defer image fetch until needed
    q = f"'{folder_id}' in parents and trashed = false"
    files = drive.files().list(q=q, fields="files(id,name,mimeType,size)").execute().get("files", [])
    return {f["name"]: f["id"] for f in files}

def main():
    st.title("Image Filtering")

    # --- LOGIN ---
    users = st.secrets["users"]
    with st.sidebar:
        st.header("Login")
        username = st.text_input("Name")
        password = st.text_input("Password", type="password")
    if not username or users.get(username) != password:
        st.info("Enter valid credentials to continue.")
        st.stop()

    # --- Drive init is lazy; only if we have secrets ---
    gcp = st.secrets["gcp_service_account"]
    drive = get_drive_client(gcp)

    # --- Small health check (does not scan everything) ---
    with st.sidebar:
        st.header("Data source")
        folder_id = st.text_input("Google Drive folder ID (root that contains dataset/images/*)")
        if st.button("Test connection"):
            try:
                _ = list_folder_files(drive, folder_id)
                st.success("Drive reachable ✅")
            except Exception as e:
                st.error(f"Drive error: {e}")
                st.stop()

    # Defer the rest of your UI… (load category → then list one JSONL → then load 1 pair)
    # ...

if __name__ == "__main__":
    main()

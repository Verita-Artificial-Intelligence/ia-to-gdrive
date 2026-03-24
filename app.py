import streamlit as st
import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import Flow

# Import the core logic from our command line script
from ia_books_to_gdrive import (
    BookQuery,
    search_ia,
    find_best_match,
    stream_book_to_gdrive,
    extract_folder_id,
    DEFAULT_THRESHOLD,
)

SCOPES = ["https://www.googleapis.com/auth/drive"]
CLIENT_SECRETS_FILE = "credentials.json"


# ── PKCE Verifier Store ──────────────────────────────────────────────
# st.session_state is WIPED when the user navigates away to Google Auth
# and then redirected back (the WebSocket disconnects during the full-page
# navigation). Instead, we use a server-process-global dict keyed by the
# OAuth `state` parameter, which Google always echoes back in the redirect
# URL. This dict lives as long as the Streamlit server process.
@st.cache_resource
def _get_verifier_store() -> dict:
    """Process-global store: {oauth_state: code_verifier}."""
    return {}


def init_oauth_flow():
    # Detect the current Streamlit URL to set the exact redirect URI
    try:
        host = st.context.headers.get("Host", "localhost:8501")
        protocol = "https" if "localhost" not in host else "http"
        redirect_uri = f"{protocol}://{host}/"
    except Exception:
        redirect_uri = "http://localhost:8501/"

    redirect_uri = os.environ.get("REDIRECT_URI", redirect_uri)

    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )
    return flow


def main():
    st.set_page_config(page_title="IA to Google Drive", page_icon="📚", layout="centered")

    st.title("📚 Internet Archive to Google Drive Streamer")
    st.markdown("Seamlessly transfer books from Internet Archive directly to your Google Drive via zero-disk streaming.")

    # 1. OAuth State Management
    if "credentials" not in st.session_state:
        st.session_state.credentials = None

    verifier_store = _get_verifier_store()

    # Handle OAuth callback redirect
    if "code" in st.query_params:
        try:
            code = st.query_params["code"]
            oauth_state = st.query_params.get("state")
            flow = init_oauth_flow()

            # Restore the PKCE code_verifier from our process-global store.
            # The key is the `state` param that Google echoed back.
            code_verifier = verifier_store.pop(oauth_state, None)

            # Pass code_verifier explicitly so it is included in the token
            # exchange request body, satisfying Google's PKCE validation.
            flow.fetch_token(code=code, code_verifier=code_verifier)

            st.session_state.credentials = flow.credentials.to_json()
            st.query_params.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Authentication callback failed: {e}")
            st.query_params.clear()

    # 2. Authentication UI
    if not st.session_state.credentials:
        st.info("👋 Welcome! To upload books directly to your Google Drive, please authenticate first.")

        if not os.path.exists(CLIENT_SECRETS_FILE):
            st.error(f"🚨 Missing `{CLIENT_SECRETS_FILE}` in the working directory.")
            st.markdown("Please generate an OAuth 2.0 Client ID of type **Web application** from the [Google Cloud Console](https://console.cloud.google.com/), download it, name it `credentials.json`, and place it in the project root.")
            return

        try:
            flow = init_oauth_flow()
            auth_url, state = flow.authorization_url(prompt="consent")

            # Persist the PKCE code_verifier in our process-global store,
            # keyed by `state` which Google will echo back after consent.
            if hasattr(flow, "code_verifier") and flow.code_verifier:
                verifier_store[state] = flow.code_verifier

            st.link_button("🔑 Login with Google", auth_url, type="primary")
        except ValueError as e:
            st.error(f"Configuration Error: {e}\n\nMake sure your `credentials.json` is a **Web application** type, not a Desktop client.")
        except Exception as e:
            st.error(f"Error configuring OAuth: {e}")
        return

    # Load active credentials
    creds = Credentials.from_authorized_user_info(json.loads(st.session_state.credentials))
    service = build("drive", "v3", credentials=creds)

    # 3. Sidebar Configuration
    with st.sidebar:
        st.success("✅ Authenticated")
        if st.button("Logout"):
            st.session_state.credentials = None
            st.rerun()
            
        st.divider()
        st.markdown("### Settings")
        threshold = st.slider("Match Threshold", min_value=0, max_value=100, value=int(DEFAULT_THRESHOLD), help="Minimum fuzzy match score (0-100) required to stream an IA item.")

    # 4. Main Application UI
    with st.form("upload_form"):
        folder_input = st.text_input(
            "Target Google Drive Folder",
            placeholder="Paste folder ID or full sharing link (e.g. https://drive.google.com/drive/folders/...)",
            help="Leave blank to upload to your root My Drive."
        )
        
        books_input = st.text_area(
            "Books to Upload",
            placeholder="Moby Dick | Herman Melville\nThe Great Gatsby\nMeditations | Marcus Aurelius",
            height=200,
            help="One book per line. Optional: Use a pipe | to specify the author."
        )
        
        submit = st.form_submit_button("🚀 Start Streaming", type="primary")

    if submit:
        if not books_input.strip():
            st.warning("Please enter at least one book.")
            return

        folder_id = extract_folder_id(folder_input)
        
        # Verify Folder Access BEFORE streaming
        if folder_id:
            try:
                meta = service.files().get(fileId=folder_id, fields="id, name, mimeType", supportsAllDrives=True).execute()
                if meta.get("mimeType") != "application/vnd.google-apps.folder":
                    st.error("Error: The provided ID is a file, not a folder.")
                    return
                st.success(f"📂 Verified Target Folder: **{meta.get('name')}**")
            except Exception as e:
                st.error(f"❌ Cannot access Google Drive folder. Make sure the link is correct and you have Editor access. Detail: {e}")
                return

        # Parse queries
        queries = []
        for line in books_input.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'): continue
            
            if '|' in line:
                title, author = line.split('|', 1)
                queries.append(BookQuery(title=title.strip(), author=author.strip() or None))
            else:
                queries.append(BookQuery(title=line, author=None))

        if not queries:
            st.warning("No valid book entries found.")
            return

        st.markdown("### Process Log")
        progress_bar = st.progress(0)
        
        results_log = []

        # Streaming Loop
        for i, query in enumerate(queries):
            display_name = f'"{query.title}"' + (f" by {query.author}" if query.author else "")
            
            with st.status(f"Processing {display_name}...", expanded=True) as status:
                # 1. Search
                st.write("🔍 Searching Internet Archive...")
                ia_results, search_ok = search_ia(query)
                
                if not search_ok:
                    status.update(label=f"❌ Failed: {display_name} (Search Engine Error)", state="error")
                    results_log.append({"Book": display_name, "Status": "Search Failed"})
                    continue
                if not ia_results:
                    status.update(label=f"⚠️ No results: {display_name}", state="error")
                    results_log.append({"Book": display_name, "Status": "Not Found"})
                    continue
                    
                # 2. Match
                match = find_best_match(query, ia_results, threshold=threshold)
                if not match:
                    status.update(label=f"⚠️ No strong match: {display_name} (Top score: {ia_results[0].get('score', 0)})", state="error")
                    results_log.append({"Book": display_name, "Status": "Below Threshold"})
                    continue
                    
                # Cap score at 100 since author_bonus can push it to 115 theoretically
                display_score = min(100.0, match['score'])
                ia_url = f"https://archive.org/details/{match['identifier']}"
                st.write(f"✅ Matched: **{match['title']}** [{ia_url}]({ia_url}) (Score: {display_score} Out of 100)")
                
                # 3. Stream
                st.write("☁️ Streaming upload to Google Drive...")
                chunk_progress = st.progress(0)
                
                def update_progress(p):
                    chunk_progress.progress(p / 100.0)
                
                # Create a status log container for live error messages
                status_log = st.empty()
                
                def log_status(msg):
                    status_log.warning(f"⚠️ {msg}")
                
                drive_file, direct_url, dl_status, err_detail = stream_book_to_gdrive(
                    service, 
                    match["identifier"], 
                    folder_id, 
                    progress_callback=update_progress,
                    status_callback=log_status,
                )
                
                if not drive_file:
                    err_msg = f"❌ Upload Failed: {display_name}"
                    if err_detail:
                        err_msg += f" — {err_detail}"
                    status.update(label=err_msg, state="error")
                    st.error(f"**Error detail:** {err_detail}")
                    results_log.append({"Book": display_name, "Status": f"Failed: {err_detail}"})
                    continue
                    
                chunk_progress.progress(1.0) # Ensure it shows 100%
                st.write(f"🎉 **Success!** File ID: `{drive_file['id']}`")
                status.update(label=f"✅ Uploaded: {display_name}", state="complete")
                results_log.append({
                    "Book": query.title, 
                    "Matched IA Title": match['title'], 
                    "Status": "✅ Success", 
                    "Drive ID": drive_file['id']
                })
                
            # Update overall progress
            progress_bar.progress((i + 1) / len(queries))
            
        st.balloons()
        st.success("All tasks completed!")
        st.dataframe(results_log, use_container_width=True)

if __name__ == "__main__":
    main()

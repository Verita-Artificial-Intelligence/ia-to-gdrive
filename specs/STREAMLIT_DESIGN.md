# Streamlit App Design: Internet Archive to Google Drive

## 1. Executive Summary
This document outlines the architecture, user interface (UI), and deployment considerations for wrapping the `ia_books_to_gdrive.py` streaming pipeline into a highly interactive, cloud-deployable Streamlit application. The design strictly adheres to the "truly on cloud" ethos, ensuring zero local disk footprints and accommodating Streamlit Community Cloud constraints.

## 2. Core Constraints & Cloud Deployment
Streamlit Community Cloud environments impose unique challenges regarding OAuth flows and execution state:
- **No Localhost OAuth**: The `InstalledAppFlow` (opening a browser to localhost for Google Auth) **fails** in remote cloud containers.
- **Stateless Execution**: Streamlit apps re-run top-to-bottom on every UI interaction. Long-running streaming pipelines must be insulated via `@st.experimental_fragment` or `st.spinner` / state management to avoid interruption.
- **Strict RAM/Disk limits**: Our previous `IARangeStream` implementation perfectly solves the 800MB disk limit and 1GB RAM limit.

## 3. Recommended Auth Architecture (Researched Best Practices)

Authenticating the Google Drive API in a Streamlit environment requires careful handling of `credentials.json` to avoid catastrophic security leaks (e.g., committing secrets to GitHub) and to ensure compatibility with stateless remote containers where localhost browser redirects (`InstalledAppFlow`) fail. 

Based on official Streamlit documentation and rigorous security best practices, here are the three viable architectures we will support, ordered by recommendation:

### Approach A: Streamlit Secrets Management (Gold Standard for Cloud Apps)
**Target**: Apps deployed to Streamlit Community Cloud for a specific pre-determined Google Drive destination.
- **Mechanism**: The contents of a Google Cloud **Service Account** JSON file (`credentials.json`) are copied into Streamlit's `.streamlit/secrets.toml` file or the Cloud Secrets GUI workspace. 
- **Code implementation**: 
  ```python
  from google.oauth2.service_account import Credentials
  cred_dict = dict(st.secrets["gcp_service_account"])
  creds = Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
  ```
- **Operational Reality**: Since Service Accounts are distinct virtual users, the actual human user MUST share their target Google Drive Folder explicitly with the Service Account's generated email address (`bot@my-project.iam.gserviceaccount.com`). This ensures perfect authorization without expiring refresh tokens.

### Approach B: Web-Server OAuth Flow (Best for Multi-User/Public Apps)
**Target**: Making this tool available publicly so ANY user can upload to THEIR OWN Google Drive.
- **Mechanism**: Use the `streamlit-oauth` library or `google-auth-oauthlib.flow.Flow` (configured for web). 
- **Operational Reality**: The UI provides a "Login with Google" button. The app redirects the user to Google's consent screen, captures the authorization code via `st.query_params`, exchanges it for a token, and stores the user's ephemeral token in `st.session_state`. We never manage permanent `credentials.json` files; we only store the app's standard Client ID/Secret securely in `st.secrets`. 

### Approach C: Drag-and-Drop Token Auth (Best for Local Personal Scripts)
**Target**: A single developer running the app via `streamlit run app.py` locally or privately relying on their existing authentication.
- **Mechanism**: Provide `st.file_uploader` widgets allowing the user to upload their preexisting, locally generated `token.json` that was created from their `credentials.json`. 
- **Operational Reality**: The script reads the byte stream of the uploaded JSON seamlessly into memory to construct the `Credentials` object. This strictly avoids saving any authenticated artifacts to the server's disk, aligning beautifully with our overarching Zero Disk Footprint goal.

## 4. User Interface (UI) Design

### Sidebar: Configuration
- **Auth Mode**: Radio button selecting "Service Account" or "OAuth Token".
- **File Uploaders**: For the JSON auth file(s).
- **Match Threshold**: A numeric slider (0-100) to configure the raw fuzzy-match threshold (Default: `75`).

### Main Stage: Input & Execution
- **Header**: "IA -> GDrive True Streamer" + concise instructions.
- **Target Folder**: `st.text_input` accepting a full Google Drive URL (leveraging our new `extract_folder_id` helper).
- **Books Input**: `st.text_area` replacing the `books.txt` file. Users can simply paste their list (`Title | Author \n ...`).
- **Start Button**: `st.button("Stream to Drive", type="primary")`

### Live Progress Indicators
During execution, we must provide realtime visual feedback to prevent users from thinking the 500MB stream has frozen:
- **Global Progress Bar**: `st.progress(completed_books / total_books)`
- **Active Book Expander**: An expanding `st.status` block for the currently streaming book:
  - Phase 1: "*Searching IA for 'Moby Dick'...*"
  - Phase 2: "*Matched: Moby Dick (Score: 115.0)*"
  - Phase 3: "*Streaming 22MB to Google Drive...*" (With dynamic `%` chunk updates).
  
### Output Stage
- **Results Table**: Once the queue finishes, output the `results` array using `st.dataframe`, styling failed rows in red and successes in green.
- **CSV Export**: Replace the local `report.csv` file with a `st.download_button` that serves the CSV natively from memory (converted via `pandas`).

## 5. Refactoring Requirements
To seamlessly bind our CLI script to a Streamlit UI, `ia_books_to_gdrive.py` needs minor structural refactoring:
- **Callback Injection**: Inject a logging callback (e.g., `logger(msg: str, progress: float=None)`) into `stream_book_to_gdrive` and `run()`. Streamlit will inject a function that updates `st.write()` and `st.progress()` instead of relying on standard `print()`.
- **In-Memory Credentials**: Update `get_drive_service` to optionally accept raw JSON dictionaries (from `st.file_uploader`) rather than strictly absolute file paths.

## 6. Implementation Phases
- **Phase A**: Refactor `ia_books_to_gdrive.py` slightly to decouple `print()` statements into a configurable `logger` callback and handle in-memory Google Auth from dictionary objects.
- **Phase B**: Create `app.py` importing Streamlit, building the UI layout described.
- **Phase C**: Wire the UI inputs to the decoupled runner function, ensuring `st.status` visually reports the live streaming chunk progress.

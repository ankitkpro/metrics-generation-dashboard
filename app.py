import streamlit as st
from pymongo import MongoClient
import os
import tempfile
import moviepy.editor as mp
from google.cloud import storage
from datetime import timedelta
import json
import time
import requests
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

GCS_BUCKET_NAME = "qr-ai"
GCS_PARENT_FOLDER_NAME = "cricket_batting/"
# SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
SERVICE_ACCOUNT_FILE_JSON = st.secrets("SERVICE_ACCOUNT_FILE")
SERVICE_ACCOUNT_FILE = json.loads(SERVICE_ACCOUNT_FILE_JSON)


# BASE_URL = os.getenv("FLYTE_API_BASE_URL")
BASE_URL = st.secrets("FLYTE_API_BASE_URL")
API_KEY = os.getenv("FLYTE_API_KEY")

# MongoDB connection
# connection_string = os.getenv("MONGO_CONNECTION_STRING")
connection_string = st.secrets("MONGO_CONNECTION_STRING")
client = MongoClient(connection_string)
db = client['kpro']
gallery_collection = db['gallery']
users_collection = db['users']

# ============================================================================
# GCS UTILITY FUNCTIONS
# ============================================================================

def get_gcs_client():
    """Get authenticated GCS client using service account credentials"""
    try:
        storage_client = storage.Client.from_service_account_info(SERVICE_ACCOUNT_FILE)
        print("Using GCS service account: kpro-ai-video-json-access@kpro-staging.iam.gserviceaccount.com")
        return storage_client
    except Exception as e:
        print(f"Error authenticating with GCS: {e}")
        raise Exception(f"Could not authenticate with GCS: {str(e)}")

def parse_gsutil_url(gsutil_url):
    """
    Parse a gsutil URL (gs://) to extract bucket name and object path.
    
    Args:
        gsutil_url (str): The gsutil URL (e.g., gs://bucket-name/path/to/object)
    
    Returns:
        tuple: (bucket_name, object_name)
    """
    if gsutil_url.startswith("gs://"):
        path = gsutil_url[5:]
    else:
        path = gsutil_url
    
    parts = path.split("/", 1)
    bucket_name = parts[0]
    object_name = parts[1] if len(parts) > 1 else ""
    
    return bucket_name, object_name

def check_gcs_file_exists(bucket_name, blob_name):
    """Check if a file exists in GCS bucket"""
    try:
        storage_client = get_gcs_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        print(f"Error checking GCS file: {e}")
        return False

def download_gcs_file_to_bytes(bucket_name, blob_name):
    """Download a file from GCS bucket and return bytes"""
    try:
        storage_client = get_gcs_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        video_bytes = blob.download_as_bytes()
        return video_bytes
    except Exception as e:
        print(f"Error downloading GCS file: {e}")
        return None

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, signed_url_flag=False):
    """
    Uploads a file to GCS bucket and returns its public URL.
    
    Args:
        bucket_name (str): The ID of your bucket.
        source_file_name (str): Path to local file to upload.
        destination_blob_name (str): The storage object name in the bucket.
        signed_url_flag (bool): Whether to generate a signed URL (default False).
    
    Returns:
        str or tuple: The public URL, or (signed_url, public_url) if signed_url_flag=True
    """
    try:
        if not os.path.exists(source_file_name):
            print(f"Local file not found: {source_file_name}")
            return None
            
        storage_client = get_gcs_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        file_exists = check_gcs_file_exists(bucket_name, destination_blob_name)
        if file_exists:
            public_url = blob.public_url
            print(f"File {source_file_name} already exists in gs://{bucket_name}/{destination_blob_name}.")
            print(f"Public URL: {public_url}")
        else:
            blob.upload_from_filename(source_file_name)
            print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}.")

        # Generate signed URL if requested
        if signed_url_flag:
            try:
                signed_url = blob.generate_signed_url(expiration=timedelta(days=7))
                print(f"Signed URL: {signed_url}")
                public_url = blob.public_url
                return signed_url, public_url
            except Exception as e:
                print(f"Could not generate signed URL: {e}")
                public_url = blob.public_url
                return public_url
        else:
            public_url = blob.public_url
            print(f"Public URL: {public_url}")
            return public_url
            
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return None

def create_gcs_folder(bucket_name, folder_name):
    """
    Creates a 'folder' in GCS bucket by uploading a zero-length object with a trailing slash.
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        folder_name (str): The name of the folder to create (will add trailing slash if needed).
    
    Returns:
        str: The full GCS path to the created folder.
    """
    try:
        if not folder_name.endswith('/'):
            folder_name += '/'
        
        storage_client = get_gcs_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(folder_name)
        
        if blob.exists():
            print(f"Folder already exists: gs://{bucket_name}/{folder_name}")
        else:
            blob.upload_from_string(b'')
            print(f"Folder created: gs://{bucket_name}/{folder_name}")
        
        gcs_path = f"gs://{bucket_name}/{folder_name}"
        return gcs_path
    except Exception as e:
        print(f"Error creating GCS folder: {e}")
        return None

# ============================================================================
# FLYTE API FUNCTIONS
# ============================================================================

def build_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    if API_KEY:
        headers["x-api-key"] = API_KEY
    return headers

def run_task(payload: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    """Submit a task to Flyte"""
    tries = 3
    while tries >= 0:
        try:
            tries -= 1
            url = f"{BASE_URL}/run_task"
            response = requests.post(url, headers=build_headers(), json=payload, timeout=timeout_s)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error running task: {e}")
            time.sleep(10)
            if tries == 0:
                raise Exception(f"Failed to run task after 3 tries")
            continue

def fetch_task_execution_post(payload: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    """Fetch the status of a Flyte task execution"""
    tries = 3
    while tries >= 0:
        try:
            tries -= 1
            url = f"{BASE_URL}/fetch_task_execution"
            response = requests.post(url, headers=build_headers(), json=payload, timeout=timeout_s)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching task execution: {e}")
            time.sleep(10)
            if tries == 0:
                raise Exception(f"Failed to fetch task execution after 3 tries")
            continue

# ============================================================================
# VIDEO PROCESSING FUNCTIONS
# ============================================================================

def process_clip_to_bytes(video_bytes, clip_name):
    """Process video bytes to make them playable in browser"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_input:
            temp_input.write(video_bytes)
            temp_input_path = temp_input.name
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_output:
            temp_output_path = temp_output.name
        
        try:
            clip = mp.VideoFileClip(temp_input_path)
            clip.write_videofile(temp_output_path, codec='libx264', audio=True, remove_temp=True, verbose=False, logger=None)
            clip.close()
            
            with open(temp_output_path, 'rb') as f:
                processed_bytes = f.read()
            
            return processed_bytes
        finally:
            if os.path.exists(temp_input_path):
                os.unlink(temp_input_path)
            if os.path.exists(temp_output_path):
                os.unlink(temp_output_path)
                
    except Exception as e:
        print(f"Error processing clip {clip_name}: {e}")
        return None

# ============================================================================
# DATABASE & DRILL FUNCTIONS
# ============================================================================

def search_assessment(assessment_id):
    """Search for player and drills by assessment ID"""
    try:
        user_query = {"_id": assessment_id}
        user_result = users_collection.find_one(user_query)
        
        if not user_result:
            return None, None, "Assessment ID not found"
        
        player_full_name = user_result.get('name', 'Unknown')
        
        gallery_query = {"user_id": assessment_id}
        gallery_results = gallery_collection.find(gallery_query)
        
        drill_data = {}
        for doc in gallery_results:
            drill = doc.get('title', '')
            clips = doc.get('clips', [])
            
            # Determine drill type
            if 'top' in drill.lower():
                drill_type = 'tophand'
            elif 'bottom' in drill.lower():
                drill_type = 'bottomhand'
            elif 'drive' in drill.lower():
                drill_type = 'backfoot_drive'
            elif 'defense' in drill.lower():
                drill_type = 'backfoot_defense'
            elif 'hold' in drill.lower():
                drill_type = 'hold_your_pose'
            elif 'planted' in drill.lower():
                drill_type = 'feet_planted'
            elif 'power' in drill.lower():
                drill_type = 'power_hitting'
            elif 'run' in drill.lower():
                drill_type = 'running_bw_wickets'
            else:
                drill_type = drill.lower().replace(' ', '_')
            
            if clips:
                drill_data[drill_type] = {
                    'title': drill,
                    'clips': clips
                }
        
        if not drill_data:
            return player_full_name, None, "No drills found for this assessment"
        
        return player_full_name, drill_data, None
        
    except Exception as e:
        return None, None, f"Error: {str(e)}"

def download_and_process_clips(assessment_id, player_full_name, drill_type, clips):
    """Download and process clips for a specific drill type, using cache if available"""
    try:
        cache_key = f"{assessment_id}_{drill_type}"
        
        if cache_key in st.session_state.clips_cache:
            st.info("📦 Loading clips from cache...")
            return st.session_state.clips_cache[cache_key], None
        
        player_name = player_full_name.lower().replace(' ', '_')
        processed_clips = []
        
        with st.spinner(f'Downloading and processing {len(clips)} clips...'):
            progress_bar = st.progress(0)
            
            for i, clip in enumerate(clips):
                bucket_name, video_object_name = parse_gsutil_url(clip)
                clip_name = f"{player_name}_{drill_type}_clip_{i+1}.{video_object_name.split('.')[-1]}"
                
                video_bytes = download_gcs_file_to_bytes(bucket_name, video_object_name)
                
                if video_bytes:
                    processed_bytes = process_clip_to_bytes(video_bytes, clip_name)
                    
                    if processed_bytes:
                        processed_clips.append({
                            'name': clip_name,
                            'bytes': processed_bytes,
                            'original_url': clip
                        })
                
                progress_bar.progress((i + 1) / len(clips))
            
            progress_bar.empty()
        
        st.session_state.clips_cache[cache_key] = processed_clips
        
        return processed_clips, None
    except Exception as e:
        return None, f"Error processing clips: {str(e)}"

def upload_selected_clips_to_gcs(assessment_id, player_name, drill_type, selected_indices, processed_clips):
    """Upload selected clips to GCS with sequential numbering"""
    try:
        player_name_formatted = player_name.lower().replace(' ', '_')
        folder_name = f"{GCS_PARENT_FOLDER_NAME}{player_name_formatted}_{assessment_id}/"
        
        create_gcs_folder(GCS_BUCKET_NAME, folder_name)
        
        signed_urls = []
        
        with st.spinner(f'Uploading {len(selected_indices)} clips to GCS...'):
            progress_bar = st.progress(0)
            
            for new_idx, original_idx in enumerate(sorted(selected_indices), start=1):
                clip_data = processed_clips[original_idx]
                
                ext = clip_data['name'].split('.')[-1]
                new_filename = f"{player_name_formatted}_{drill_type}_clip_{new_idx}.{ext}"
                
                with tempfile.NamedTemporaryFile(suffix=f'.{ext}', delete=False) as temp_file:
                    temp_file.write(clip_data['bytes'])
                    temp_file_path = temp_file.name
                
                try:
                    signed_url, public_url = upload_to_gcs(
                        GCS_BUCKET_NAME,
                        temp_file_path,
                        folder_name + new_filename,
                        signed_url_flag=True
                    )
                    signed_urls.append(signed_url)
                finally:
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                
                progress_bar.progress(new_idx / len(selected_indices))
            
            progress_bar.empty()
        
        return signed_urls, None
    except Exception as e:
        return None, f"Error uploading clips: {str(e)}"

def build_flyte_inputs(drill_type, player_name, assessment_id, signed_urls, coach_feedback=""):
    """Build Flyte task inputs based on drill type"""
    player_name_formatted = player_name.lower().replace(' ', '_')
    
    drill_configs = {
        'tophand': {
            'execution_name': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'task_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
        },
        'bottomhand': {
            'execution_name': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'task_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
        },
        'backfoot_drive': {
            'execution_name': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'task_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
        },
        'backfoot_defense': {
            'execution_name': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'task_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.drive_drills_v2.get_drive_drill_metrics_v2',
        },
        'power_hitting': {
            'execution_name': 'flyte.workflows.tasks.power_hitting_v2.get_power_hitting_metrics_v2',
            'task_id': 'flyte.workflows.tasks.power_hitting_v2.get_power_hitting_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.power_hitting_v2.get_power_hitting_metrics_v2',
        },
        'hold_your_pose': {
            'execution_name': 'flyte.workflows.tasks.hold_your_pose_v2.get_hyp_metrics_v2',
            'task_id': 'flyte.workflows.tasks.hold_your_pose_v2.get_hyp_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.hold_your_pose_v2.get_hyp_metrics_v2',
        },
    }
    
    config = drill_configs.get(drill_type, drill_configs['tophand'])
    
    if drill_type == 'backfoot_drive':
        actual_drill_type = 'backfoot'
        has_defense = False
    elif drill_type == 'backfoot_defense':
        actual_drill_type = 'backfoot'
        has_defense = False
    else:
        actual_drill_type = drill_type
        has_defense = False
    
    inputs = {
        "execution_name": config['execution_name'],
        "task_id": config['task_id'],
        "launch_plan_id": config['launch_plan_id'],
        "wait_for_completion": False,
        "inputs": {
            "coach_feedback": coach_feedback,
            "debug": False,
            "player_name": player_name_formatted,
            "videos": signed_urls,
            "assessment_id": assessment_id
        }
    }
    
    if drill_type in ['tophand', 'bottomhand', 'backfoot_drive', 'backfoot_defense']:
        inputs["inputs"]["drill_type"] = actual_drill_type
        inputs["inputs"]["backfoot_defense_videos"] = []
    
    return inputs

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

if 'searched' not in st.session_state:
    st.session_state.searched = False
if 'player_name' not in st.session_state:
    st.session_state.player_name = None
if 'drill_data' not in st.session_state:
    st.session_state.drill_data = {}
if 'selected_drill' not in st.session_state:
    st.session_state.selected_drill = None
if 'processed_clips' not in st.session_state:
    st.session_state.processed_clips = []
if 'clips_cache' not in st.session_state:
    st.session_state.clips_cache = {}
if 'selected_clips' not in st.session_state:
    st.session_state.selected_clips = {}
if 'signed_urls' not in st.session_state:
    st.session_state.signed_urls = {}
if 'execution_ids' not in st.session_state:
    st.session_state.execution_ids = {}
if 'task_status' not in st.session_state:
    st.session_state.task_status = {}

# ============================================================================
# STREAMLIT APP UI
# ============================================================================

st.set_page_config(page_title="Cricket Batting Drills Viewer", layout="wide")
st.title("🏏 Cricket Batting Drills Viewer")

# Input section
st.header("Search Assessment")
col1, col2 = st.columns([3, 1])

with col1:
    assessment_id = st.text_input(
        "Enter Assessment ID", 
        value="4c2dfb55-9531-45ec-8381-08782ea65983",
        placeholder="e.g., 4c2dfb55-9531-45ec-8381-08782ea65983"
    )

with col2:
    st.write("")
    st.write("")
    search_button = st.button("🔍 Search", type="primary", use_container_width=True)

# Search functionality
if search_button and assessment_id:
    with st.spinner('Searching...'):
        player_name, drill_data, error = search_assessment(assessment_id)
    
    if error:
        st.error(error)
        st.session_state.searched = False
    else:
        st.session_state.searched = True
        st.session_state.player_name = player_name
        st.session_state.drill_data = drill_data
        st.session_state.assessment_id = assessment_id
        st.session_state.selected_drill = None
        st.session_state.processed_clips = []
        st.session_state.selected_clips = {}
        st.session_state.signed_urls = {}
        st.session_state.execution_ids = {}
        st.session_state.task_status = {}
        st.success(f"Found assessment for {player_name}")

# Display results
if st.session_state.searched:
    st.divider()
    
    st.header(f"Player: {st.session_state.player_name}")
    
    st.subheader("Available Drills")
    
    drill_types = list(st.session_state.drill_data.keys())
    
    if drill_types:
        cols = st.columns(min(len(drill_types), 4))
        
        for idx, drill_type in enumerate(drill_types):
            with cols[idx % 4]:
                drill_info = st.session_state.drill_data[drill_type]
                num_clips = len(drill_info['clips'])
                
                if st.button(
                    f"📹 {drill_type.title()}\n({num_clips} clips)",
                    key=f"drill_{drill_type}",
                    use_container_width=True
                ):
                    st.session_state.selected_drill = drill_type
                    st.session_state.processed_clips = []
        
        if st.session_state.selected_drill:
            st.divider()
            drill_type = st.session_state.selected_drill
            drill_info = st.session_state.drill_data[drill_type]
            
            st.subheader(f"Drill: {drill_info['title']}")
            
            if not st.session_state.processed_clips:
                processed_clips, error = download_and_process_clips(
                    st.session_state.assessment_id,
                    st.session_state.player_name,
                    drill_type,
                    drill_info['clips']
                )
                
                if error:
                    st.error(error)
                else:
                    st.session_state.processed_clips = processed_clips
                    st.success(f"Processed {len(processed_clips)} clips!")
            
            if st.session_state.processed_clips:
                st.write(f"**Showing {len(st.session_state.processed_clips)} clips**")
                
                if drill_type not in st.session_state.selected_clips:
                    st.session_state.selected_clips[drill_type] = []
                
                clips_to_show = st.session_state.processed_clips[:9]
                
                for row in range(0, len(clips_to_show), 3):
                    cols = st.columns(3)
                    
                    for col_idx, clip_data in enumerate(clips_to_show[row:row+3]):
                        clip_idx = row + col_idx
                        with cols[col_idx]:
                            is_selected = st.checkbox(
                                f"Select Clip {clip_idx + 1}",
                                value=clip_idx in st.session_state.selected_clips[drill_type],
                                key=f"select_{drill_type}_{clip_idx}"
                            )
                            
                            if is_selected and clip_idx not in st.session_state.selected_clips[drill_type]:
                                st.session_state.selected_clips[drill_type].append(clip_idx)
                            elif not is_selected and clip_idx in st.session_state.selected_clips[drill_type]:
                                st.session_state.selected_clips[drill_type].remove(clip_idx)
                            
                            clip_name = clip_data['name']
                            clip_bytes = clip_data['bytes']
                            st.write(f"**Clip {clip_idx + 1}**")
                            st.video(clip_bytes)
                
                if len(st.session_state.processed_clips) > 9:
                    st.info(f"Showing first 9 of {len(st.session_state.processed_clips)} clips")
                
                # Flyte Task Workflow Section
                st.divider()
                st.subheader("🚀 Run Flyte Task")
                
                num_selected = len(st.session_state.selected_clips.get(drill_type, []))
                st.write(f"**Selected Clips:** {num_selected}")
                
                if num_selected > 0:
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        upload_button = st.button(
                            "☁️ Upload to GCS",
                            key=f"upload_{drill_type}",
                            use_container_width=True,
                            disabled=drill_type in st.session_state.signed_urls
                        )
                    
                    if upload_button:
                        signed_urls, error = upload_selected_clips_to_gcs(
                            st.session_state.assessment_id,
                            st.session_state.player_name,
                            drill_type,
                            st.session_state.selected_clips[drill_type],
                            st.session_state.processed_clips
                        )
                        
                        if error:
                            st.error(error)
                        else:
                            st.session_state.signed_urls[drill_type] = signed_urls
                            st.success(f"✅ Uploaded {len(signed_urls)} clips to GCS!")
                    
                    if drill_type in st.session_state.signed_urls:
                        st.success(f"✅ {len(st.session_state.signed_urls[drill_type])} clips uploaded to GCS")
                        
                        coach_feedback = st.text_area(
                            "Coach Feedback (optional)",
                            key=f"feedback_{drill_type}",
                            placeholder="Enter any coach feedback here..."
                        )
                        
                        flyte_inputs = build_flyte_inputs(
                            drill_type,
                            st.session_state.player_name,
                            st.session_state.assessment_id,
                            st.session_state.signed_urls[drill_type],
                            coach_feedback
                        )
                        
                        st.write("**Flyte Task Inputs:**")
                        st.json(flyte_inputs, expanded=False)
                        
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            run_button = st.button(
                                "▶️ Run Task",
                                key=f"run_{drill_type}",
                                use_container_width=True,
                                disabled=drill_type in st.session_state.execution_ids
                            )
                        
                        if run_button:
                            with st.spinner("Submitting task..."):
                                try:
                                    response = run_task(flyte_inputs)
                                    execution_id = response.get("execution_id")
                                    st.session_state.execution_ids[drill_type] = execution_id
                                    st.session_state.task_status[drill_type] = {
                                        "execution_id": execution_id,
                                        "status": "submitted",
                                        "is_done": False
                                    }
                                    st.success(f"✅ Task submitted! Execution ID: {execution_id}")
                                except Exception as e:
                                    st.error(f"Error running task: {str(e)}")
                        
                        if drill_type in st.session_state.execution_ids:
                            st.divider()
                            st.subheader("📊 Task Status")
                            
                            execution_id = st.session_state.execution_ids[drill_type]
                            st.write(f"**Execution ID:** `{execution_id}`")
                            
                            col1, col2 = st.columns([1, 3])
                            with col1:
                                check_status_button = st.button(
                                    "🔄 Check Status",
                                    key=f"status_{drill_type}",
                                    use_container_width=True
                                )
                            
                            if check_status_button:
                                with st.spinner("Fetching status..."):
                                    try:
                                        status_response = fetch_task_execution_post({"execution_id": execution_id})
                                        st.session_state.task_status[drill_type] = status_response
                                    except Exception as e:
                                        st.error(f"Error fetching status: {str(e)}")
                            
                            if drill_type in st.session_state.task_status:
                                status_data = st.session_state.task_status[drill_type]
                                is_done = status_data.get("is_done", False)
                                
                                if is_done:
                                    st.success("✅ Task Completed!")
                                else:
                                    st.info("⏳ Task In Progress...")
                                
                                with st.expander("View Full Status", expanded=is_done):
                                    st.json(status_data)
                else:
                    st.info("👆 Select clips above to run Flyte task")
    else:
        st.warning("No drills found")

# Sidebar with info
with st.sidebar:
    st.header("ℹ️ About")
    st.write("""
    This app allows you to:
    1. Search for assessments by ID
    2. View player information
    3. Browse available drill types
    4. View drill clips in a grid
    5. Select clips for Flyte tasks
    6. Upload clips to GCS
    7. Run Flyte tasks with custom inputs
    8. Monitor task execution status
    """)
    
    if st.session_state.searched:
        st.divider()
        st.subheader("Current Session")
        st.write(f"**Player:** {st.session_state.player_name}")
        if st.session_state.selected_drill:
            st.write(f"**Selected Drill:** {st.session_state.selected_drill}")
            st.write(f"**Clips:** {len(st.session_state.processed_clips)}")
        
        st.divider()
        st.subheader("Flyte Tasks")
        num_tasks = len(st.session_state.execution_ids)
        st.write(f"**Running/Completed Tasks:** {num_tasks}")
        if num_tasks > 0:
            for drill, exec_id in st.session_state.execution_ids.items():
                status_emoji = "✅" if st.session_state.task_status.get(drill, {}).get("is_done", False) else "⏳"
                st.write(f"{status_emoji} {drill}")
        
        st.divider()
        st.subheader("Selected Clips")
        total_selected = sum(len(clips) for clips in st.session_state.selected_clips.values())
        st.write(f"**Total Selected:** {total_selected}")
        if total_selected > 0:
            for drill, clips in st.session_state.selected_clips.items():
                if clips:
                    st.write(f"- {drill}: {len(clips)} clips")
        
        st.divider()
        st.subheader("Cache Info")
        num_cached = len(st.session_state.clips_cache)
        st.write(f"**Cached Drills:** {num_cached}")
        if num_cached > 0:
            for cache_key in st.session_state.clips_cache.keys():
                st.write(f"- {cache_key}")
    
    st.divider()
    if st.button("🗑️ Clear Cache & Reset", use_container_width=True):
        st.session_state.searched = False
        st.session_state.player_name = None
        st.session_state.drill_data = {}
        st.session_state.selected_drill = None
        st.session_state.processed_clips = []
        st.session_state.clips_cache = {}
        st.session_state.selected_clips = {}
        st.session_state.signed_urls = {}
        st.session_state.execution_ids = {}
        st.session_state.task_status = {}
        st.success("Cache cleared!")
        st.rerun()



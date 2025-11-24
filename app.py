import streamlit as st
from pymongo import MongoClient
import os
import tempfile
import moviepy as mp
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
SERVICE_ACCOUNT_FILE_JSON = st.secrets["SERVICE_ACCOUNT_FILE"]
# st.write(type(SERVICE_ACCOUNT_FILE_JSON))
# SERVICE_ACCOUNT_FILE = json.loads(SERVICE_ACCOUNT_FILE_JSON)
SERVICE_ACCOUNT_FILE = SERVICE_ACCOUNT_FILE_JSON
# st.write(SERVICE_ACCOUNT_FILE.keys())


# BASE_URL = os.getenv("FLYTE_API_BASE_URL")
BASE_URL = st.secrets["FLYTE_API_BASE_URL"]
API_KEY = os.getenv("FLYTE_API_KEY")

# MongoDB connection
# connection_string = os.getenv("MONGO_CONNECTION_STRING")
connection_string = st.secrets["MONGO_CONNECTION_STRING"]
client = MongoClient(connection_string)
db = client['kpro']
gallery_collection = db['gallery']
users_collection = db['users']


# MongoDB staging connection for drill results
# staging_connection_string = ''
staging_connection_string = st.secrets["MONGO_CONNECTION_STRING_STAGING"]
staging_client = MongoClient(staging_connection_string)
staging_db = staging_client['kpro']
drills_collection = staging_db['drill_results']
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
            clip.write_videofile(temp_output_path, codec='libx264', audio=True, remove_temp=True, logger=None)
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
# SCORING & METRICS FUNCTIONS
# ============================================================================

def get_score(angle, ball_direction, lefty=False):
    """Calculate score based on shot angle and ball direction"""
    shot_angle = int(angle)

    scoring_rules = {
        False: {  # Right-handed batter
            'Off Stump': [(100, 120, 5), (90, 100, 4), (120, 130, 4), (80, 90, 3), (130, 140, 3),
                          (70, 80, 2), (140, 150, 2), (60, 70, 1), (150, 160, 1)],
            'Outside Off Stump': [(110, 150, 5), (100, 110, 4), (150, 160, 4), (90, 100, 3),
                                  (160, 170, 3), (80, 90, 2), (170, 180, 2), (70, 80, 1)],
            'Middle Stump': [(80, 100, 5), (70, 80, 4), (100, 110, 4), (60, 70, 3), (110, 120, 3),
                             (50, 60, 2), (120, 130, 2), (40, 50, 1), (130, 140, 1)],
            'Leg Stump': [(60, 80, 5), (50, 60, 4), (80, 90, 4), (40, 50, 3), (90, 100, 3),
                          (30, 40, 2), (20, 30, 1)],
        },
        True: {  # Left-handed batter
            'Off Stump': [(60, 80, 5), (50, 60, 4), (80, 90, 4), (40, 50, 3), (90, 100, 3),
                          (30, 40, 2), (20, 30, 1)],
            'Outside Off Stump': [(30, 60, 5), (20, 30, 4), (60, 70, 4), (10, 20, 3),
                                  (70, 80, 3), (0, 10, 2), (80, 90, 2), (90, 100, 1)],
            'Middle Stump': [(80, 100, 5), (70, 80, 4), (100, 110, 4), (60, 70, 3), (110, 120, 3),
                             (50, 60, 2), (120, 130, 2), (40, 50, 1), (130, 140, 1)],
            'Leg Stump': [(100, 120, 5), (90, 100, 4), (120, 130, 4), (80, 90, 3), (130, 140, 3),
                          (140, 150, 2), (150, 160, 1)],
        }
    }

    for low, high, score in scoring_rules[lefty].get(ball_direction, []):
        if low < shot_angle <= high:
            return score
    return 0

def shot_direction_metrics(shot_direction_angles, batter_hand):
    """Convert shot direction angles to direction names"""
    lefty = batter_hand == 'Left'

    righty_angles = [
        ((60, 80), "Mid On"),
        ((100, 110), "Mid Off"),
        ((110, 150), "Cover"),
        ((80, 100), "Straight"),
        ((30, 60), "Mid Wicket"),
        ((0, 30), "Square Leg"),
        ((150, 180), "Point"),
    ]

    lefty_angles = [
        ((70, 80), "Mid Off"),
        ((100, 120), "Mid On"),
        ((30, 70), "Cover"),
        ((80, 100), "Straight"),
        ((0, 30), "Point"),
        ((120, 150), "Mid Wicket"),
        ((150, 180), "Square Leg"),
    ]

    angles = lefty_angles if lefty else righty_angles

    shot_directions = []
    for i in range(len(shot_direction_angles)):
        shot_direction_angle_degrees = shot_direction_angles[i]
        if shot_direction_angle_degrees <= 0 or shot_direction_angle_degrees >= 180:
            shot_directions.append('Behind the Wicket')
        else:
            found = False
            for (low, high), shot_direction in angles:
                if low < shot_direction_angle_degrees <= high:
                    shot_directions.append(shot_direction)
                    found = True
                    break
            if not found:
                shot_directions.append('Behind the Wicket')

    return shot_directions

def get_score_metrics(final_metrics, drill_type):
    """Calculate score metrics and insights"""
    ball_directions = final_metrics['ball_direction']
    shot_directions = final_metrics['shot_direction']
    scores = final_metrics['score']
    shot_accuracy = final_metrics['shot_accuracy']
    
    back_leg = []
    head_over_ball = []
    for i in range(len(final_metrics['batter_hand'])):
        if final_metrics['batter_hand'][i] == 'Right':
            if final_metrics['backleg_knee_angle'][i] > 150:
                back_leg.append('Straight')
            else:
                back_leg.append('Slightly Bent')

            if final_metrics['head_to_ball_angle'][i] < 110:
                head_over_ball.append('Yes')
            else:
                head_over_ball.append('No')

        if final_metrics['batter_hand'][i] == 'Left':
            if final_metrics['backleg_knee_angle'][i] < 30:
                back_leg.append('Straight')
            else:
                back_leg.append('Slightly Bent')

            if final_metrics['head_to_ball_angle'][i] > 70:
                head_over_ball.append('Yes')
            else:
                head_over_ball.append('No')

    percentile = int(20 * sum(scores) / len(scores))

    if percentile > 90:
        grade = 'A+'
    elif percentile > 80:
        grade = 'A'
    elif percentile > 70:
        grade = 'B+'
    elif percentile > 60:
        grade = 'B'
    elif percentile > 50:
        grade = 'C+'
    elif percentile > 40:
        grade = 'C'
    else:
        grade = 'D'

    json_dict = {}
    json_dict['percentile'] = percentile
    json_dict['grade'] = grade

    if 'back' not in drill_type:
        ball_insights = []
        ball_comments = []

        for i in range(len(ball_directions)):
            current_score = scores[i]
            if current_score == 5:
                ball_insights.append('Good')
            elif current_score == 4 or current_score == 3:
                ball_insights.append('Average')
            elif current_score < 3:
                ball_insights.append('Bad')

            if shot_directions[i] in ['Square Leg', 'Point', 'Straight', 'Mid On', 'Mid Off', 'Cover', 'Mid Wicket']:
                comment = ball_directions[i] + ' Ball Hit to ' + shot_directions[i]
            else:
                comment = ball_directions[i] + ' Ball Mistimed'

            if shot_accuracy[i] == 'Inaccurate':
                ball_comments.append(f'Inaccurate: {comment}.')
            else:
                ball_comments.append(f'Accurate: {comment}.')

        json_dict['insights'] = {
            'ball_insights': ball_insights,
            'ball_comments': ball_comments
        }

    return json_dict

def recalculate_metrics(metrics, shot_direction_angles, ball_directions, batter_hand):
    """Recalculate all metrics based on updated angles and directions"""
    # Update base values
    metrics['shot_direction_angles'] = shot_direction_angles
    metrics['ball_directions'] = ball_directions

    # Calculate scores
    scores = []
    for i in range(len(shot_direction_angles)):
        score = get_score(shot_direction_angles[i], ball_directions[i], batter_hand == 'Left')
        scores.append(score)
    
    metrics['new_metrics']['score'] = scores
    metrics['new_metrics']['shot_accuracy'] = ['Accurate' if score >= 3 else 'Inaccurate' for score in scores]

    # Calculate shot directions
    shot_directions = shot_direction_metrics(shot_direction_angles, batter_hand)
    metrics['new_metrics']['shot_direction'] = shot_directions
    metrics['new_metrics']['shot_direction_angle'] = shot_direction_angles
    metrics['new_metrics']['ball_direction'] = ball_directions

    # Calculate insights and grade
    final_metrics = metrics['new_metrics']
    drill_type = metrics.get('drill_type', '')
    score_metrics = get_score_metrics(final_metrics, drill_type)
    # If metrics['insights'] is str then skip updating insights
    if not isinstance(metrics['insights'], str):
        metrics['insights'] = score_metrics.get('insights', {})
    metrics['percentile'] = score_metrics['percentile']
    metrics['grade'] = score_metrics['grade']

    return metrics

# ============================================================================
# DATABASE & DRILL FUNCTIONS
# ============================================================================

def fetch_drill_results(assessment_id):
    """Fetch drill results from MongoDB"""
    try:
        search_query = {"assessment_id": assessment_id}
        drill_doc = drills_collection.find_one(search_query)
        return drill_doc
    except Exception as e:
        print(f"Error fetching drill results: {e}")
        return None

def update_drill_results_in_mongo(assessment_id, drill_type, updated_metrics):
    """Update drill results in MongoDB"""
    try:
        search_query = {"assessment_id": assessment_id}
        update_query = {
            "$set": {
                f"drill_metrics.{drill_type}": updated_metrics
            }
        }
        result = drills_collection.update_one(search_query, update_query)
        return result.modified_count > 0
    except Exception as e:
        print(f"Error updating drill results: {e}")
        return False

def get_mongo_drill_key(drill_type):
    """Map UI drill type to MongoDB drill type key"""
    drill_type_mapping = {
        'tophand': 'top_hand',
        'bottomhand': 'bottom_hand',
        'backfoot_drive': 'backfoot',
        'backfoot_defense': 'backfoot_defense',
        'power_hitting': 'power_hitting',
        'hold_your_pose': 'hold_your_pose',
        'running_drill': 'running_drill',
        'feet_planted': 'feet_planted'
    }
    return drill_type_mapping.get(drill_type, drill_type)

def save_coach_feedback_to_mongo(assessment_id, drill_type, coach_feedback):
    """Save only coach feedback to MongoDB for a specific drill"""
    try:
        mongo_drill_key = get_mongo_drill_key(drill_type)
        search_query = {"assessment_id": assessment_id}
        update_query = {
            "$set": {
                f"drill_metrics.{mongo_drill_key}.coach_feedback": coach_feedback
            }
        }
        result = drills_collection.update_one(search_query, update_query, upsert=True)
        return result.modified_count > 0 or result.upserted_id is not None
    except Exception as e:
        print(f"Error saving coach feedback: {e}")
        st.error(f"Error saving coach feedback: {e}")
        return False

def get_coach_feedback_from_mongo(assessment_id, drill_type):
    """Get coach feedback from MongoDB for a specific drill"""
    try:
        mongo_drill_key = get_mongo_drill_key(drill_type)
        search_query = {"assessment_id": assessment_id}
        drill_doc = drills_collection.find_one(search_query)
        if drill_doc and 'drill_metrics' in drill_doc:
            drill_metrics = drill_doc['drill_metrics']
            if mongo_drill_key in drill_metrics:
                return drill_metrics[mongo_drill_key].get('coach_feedback', '')
        return ''
    except Exception as e:
        print(f"Error fetching coach feedback: {e}")
        return ''

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
            drill_id = doc.get('drill_id', '')
            activity_id = doc.get('activity_id', '')
            
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
            elif 'run' in drill.lower() or 'batting_three_run' in drill.lower():
                drill_type = 'running_drill'
            else:
                drill_type = drill.lower().replace(' ', '_')
            
            # Handle batting_three_run (running drill) with single video
            if 'batting_three_run' in drill.lower() and drill_id and activity_id:
                video_url = f"gs://uploads.storage.khiladipro.com/{drill_id}/{activity_id}/{activity_id}.mp4"
                drill_data[drill_type] = {
                    'title': drill,
                    'video_url': video_url,
                    'drill_id': drill_id,
                    'activity_id': activity_id,
                    'is_single_video': True
                }
            elif clips:
                drill_data[drill_type] = {
                    'title': drill,
                    'clips': clips,
                    'is_single_video': False
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

def build_flyte_inputs(drill_type, player_name, assessment_id, signed_urls, coach_feedback="", defense_urls=[]):
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
        'running_drill': {
            'execution_name': 'flyte.workflows.tasks.running_drill_v2.generate_run3_metrics_v2',
            'task_id': 'flyte.workflows.tasks.running_drill_v2.generate_run3_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.running_drill_v2.generate_run3_metrics_v2',
        },
        'feet_planted': {
            'execution_name': 'flyte.workflows.tasks.feet_planted_v2.get_feet_planted_metrics_v2',
            'task_id': 'flyte.workflows.tasks.feet_planted_v2.get_feet_planted_metrics_v2',
            'launch_plan_id': 'flyte.workflows.tasks.feet_planted_v2.get_feet_planted_metrics_v2',
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
    
    # Special handling for running_drill
    if drill_type == 'running_drill':
        inputs = {
            "execution_name": config['execution_name'],
            "task_id": config['task_id'],
            "launch_plan_id": config['launch_plan_id'],
            "wait_for_completion": False,
            "inputs": {
                "video_path": signed_urls[0] if isinstance(signed_urls, list) else signed_urls,
                "player_name": player_name_formatted,
                "assessment_id": assessment_id,
                "gender": st.session_state.running_drill_inputs.get('gender', 'male'),
                "category": st.session_state.running_drill_inputs.get('category', 'u19'),
                "coach_feedback": coach_feedback,
                "debug": False
            }
        }
    else:
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
            # Add defense videos for backfoot_drive if provided
            if drill_type == 'backfoot_drive':
                inputs["inputs"]["backfoot_defense_videos"] = defense_urls if defense_urls else []
            else:
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
if 'drill_metrics_data' not in st.session_state:
    st.session_state.drill_metrics_data = {}
if 'edited_angles' not in st.session_state:
    st.session_state.edited_angles = {}
if 'edited_directions' not in st.session_state:
    st.session_state.edited_directions = {}
if 'original_values' not in st.session_state:
    st.session_state.original_values = {}
if 'mongo_drill_keys' not in st.session_state:
    st.session_state.mongo_drill_keys = {}
if 'defense_processed_clips' not in st.session_state:
    st.session_state.defense_processed_clips = {}
if 'defense_signed_urls' not in st.session_state:
    st.session_state.defense_signed_urls = {}
if 'running_drill_inputs' not in st.session_state:
    st.session_state.running_drill_inputs = {}
if 'saved_coach_feedback' not in st.session_state:
    st.session_state.saved_coach_feedback = {}

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
        st.session_state.drill_metrics_data = {}
        st.session_state.edited_angles = {}
        st.session_state.edited_directions = {}
        st.session_state.original_values = {}
        st.session_state.mongo_drill_keys = {}
        st.session_state.defense_processed_clips = {}
        st.session_state.defense_signed_urls = {}
        st.session_state.saved_coach_feedback = {}
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
                
                # Different display for single video drills
                if drill_info.get('is_single_video', False):
                    button_text = f"🏃 {drill_type.title()}\n(Single Video)"
                else:
                    num_clips = len(drill_info.get('clips', []))
                    button_text = f"📹 {drill_type.title()}\n({num_clips} clips)"
                
                if st.button(
                    button_text,
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
            
            # Handle running_drill (single video) separately
            if drill_info.get('is_single_video', False) and drill_type == 'running_drill':
                st.write("**Original Video URL:**")
                st.code(drill_info['video_url'])
                
                st.divider()
                st.subheader("🏃 Running Drill Configuration")
                
                col1, col2 = st.columns(2)
                with col1:
                    gender = st.selectbox(
                        "Gender",
                        options=["male", "female"],
                        key="running_gender"
                    )
                with col2:
                    # Different categories for male and female
                    if gender == "male":
                        category_options = ["open", "u23", "u19", "u16", "u14"]
                    else:
                        category_options = ["open", "u23", "u19", "u15"]
                    
                    category = st.selectbox(
                        "Category",
                        options=category_options,
                        key="running_category"
                    )
                
                # Store in session state
                st.session_state.running_drill_inputs['gender'] = gender
                st.session_state.running_drill_inputs['category'] = category
                
                st.divider()
                
                # Download and process the video
                if not st.session_state.processed_clips:
                    with st.spinner('Downloading and processing running drill video...'):
                        try:
                            bucket_name, video_object_name = parse_gsutil_url(drill_info['video_url'])
                            player_name = st.session_state.player_name.lower().replace(' ', '_')
                            video_name = f"{player_name}_{drill_type}_video.{video_object_name.split('.')[-1]}"
                            
                            video_bytes = download_gcs_file_to_bytes(bucket_name, video_object_name)
                            
                            if video_bytes:
                                processed_bytes = process_clip_to_bytes(video_bytes, video_name)
                                
                                if processed_bytes:
                                    st.session_state.processed_clips = [{
                                        'name': video_name,
                                        'bytes': processed_bytes,
                                        'original_url': drill_info['video_url']
                                    }]
                                    st.success("✅ Video processed successfully!")
                                else:
                                    st.error("Failed to process video")
                            else:
                                st.error("Failed to download video")
                        except Exception as e:
                            st.error(f"Error processing video: {str(e)}")
                
                # Display the processed video
                if st.session_state.processed_clips:
                    st.write("**Video Preview:**")
                    video_data = st.session_state.processed_clips[0]
                    st.video(video_data['bytes'])
                    
                    # Coach Feedback Section for Running Drill
                    st.divider()
                    st.subheader("📝 Coach Feedback (Optional)")
                    
                    # Load saved coach feedback from MongoDB if not already in session
                    feedback_key = f"{st.session_state.assessment_id}_{drill_type}"
                    if feedback_key not in st.session_state.saved_coach_feedback:
                        saved_feedback = get_coach_feedback_from_mongo(st.session_state.assessment_id, drill_type)
                        st.session_state.saved_coach_feedback[feedback_key] = saved_feedback
                    
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        coach_feedback_early_running = st.text_area(
                            "Enter coach feedback for this drill",
                            value=st.session_state.saved_coach_feedback.get(feedback_key, ''),
                            key=f"early_feedback_{drill_type}",
                            placeholder="Enter any coach feedback here...",
                            height=100
                        )
                    
                    with col2:
                        st.write("")
                        st.write("")
                        save_feedback_button_running = st.button(
                            "💾 Save Feedback",
                            key=f"save_feedback_{drill_type}",
                            use_container_width=True
                        )
                    
                    if save_feedback_button_running:
                        with st.spinner("Saving coach feedback to MongoDB..."):
                            success = save_coach_feedback_to_mongo(
                                st.session_state.assessment_id,
                                drill_type,
                                coach_feedback_early_running
                            )
                            if success:
                                st.session_state.saved_coach_feedback[feedback_key] = coach_feedback_early_running
                                st.success("✅ Coach feedback saved to MongoDB!")
                            else:
                                st.error("❌ Failed to save coach feedback")
                    
                    if st.session_state.saved_coach_feedback.get(feedback_key, ''):
                        st.info(f"💾 Saved feedback: {st.session_state.saved_coach_feedback[feedback_key][:100]}...")
                    
                    st.divider()
                    st.subheader("🚀 Run Flyte Task")
                    
                    # Upload to GCS button
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        upload_button = st.button(
                            "☁️ Upload to GCS",
                            key=f"upload_{drill_type}",
                            use_container_width=True,
                            disabled=drill_type in st.session_state.signed_urls
                        )
                    
                    if upload_button:
                        # Upload the single video to GCS
                        with st.spinner('Uploading video to GCS...'):
                            try:
                                player_name = st.session_state.player_name.lower().replace(' ', '_')
                                folder_name = f"{GCS_PARENT_FOLDER_NAME}{player_name}_{st.session_state.assessment_id}/"
                                create_gcs_folder(GCS_BUCKET_NAME, folder_name)
                                
                                # Save video bytes to temp file
                                import tempfile
                                with tempfile.NamedTemporaryFile(suffix=f'.{video_data["name"].split(".")[-1]}', delete=False) as temp_file:
                                    temp_file.write(video_data['bytes'])
                                    temp_file_path = temp_file.name
                                
                                try:
                                    signed_url, public_url = upload_to_gcs(
                                        GCS_BUCKET_NAME,
                                        temp_file_path,
                                        folder_name + video_data['name'],
                                        signed_url_flag=True
                                    )
                                    st.session_state.signed_urls[drill_type] = [signed_url]
                                    st.success(f"✅ Video uploaded to GCS!")
                                finally:
                                    if os.path.exists(temp_file_path):
                                        os.unlink(temp_file_path)
                                        
                            except Exception as e:
                                st.error(f"Error uploading video: {str(e)}")
                    
                    if drill_type in st.session_state.signed_urls:
                        st.success(f"✅ Video uploaded to GCS")
                        
                        # Auto-populate coach feedback from saved feedback
                        feedback_key = f"{st.session_state.assessment_id}_{drill_type}"
                        default_feedback = st.session_state.saved_coach_feedback.get(feedback_key, '')
                        
                        coach_feedback = st.text_area(
                            "Coach Feedback (optional)",
                            value=default_feedback,
                            key=f"feedback_{drill_type}",
                            placeholder="Enter any coach feedback here..."
                        )
                        
                        if default_feedback:
                            st.info("ℹ️ Coach feedback auto-populated from saved feedback")
                        
                        # Use the uploaded signed URL
                        flyte_inputs = build_flyte_inputs(
                            drill_type,
                            st.session_state.player_name,
                            st.session_state.assessment_id,
                            st.session_state.signed_urls[drill_type][0],  # Use the signed URL
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
                        st.info("👆 Click 'Upload to GCS' to proceed")
            
            # Handle regular multi-clip drills
            elif not drill_info.get('is_single_video', False):
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
                    
                    # Coach Feedback Section - Before Clip Selection
                    st.divider()
                    st.subheader("📝 Coach Feedback (Optional)")
                    
                    # Load saved coach feedback from MongoDB if not already in session
                    feedback_key = f"{st.session_state.assessment_id}_{drill_type}"
                    if feedback_key not in st.session_state.saved_coach_feedback:
                        saved_feedback = get_coach_feedback_from_mongo(st.session_state.assessment_id, drill_type)
                        st.session_state.saved_coach_feedback[feedback_key] = saved_feedback
                    
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        coach_feedback_early = st.text_area(
                            "Enter coach feedback for this drill",
                            value=st.session_state.saved_coach_feedback.get(feedback_key, ''),
                            key=f"early_feedback_{drill_type}",
                            placeholder="Enter any coach feedback here...",
                            height=100
                        )
                    
                    with col2:
                        st.write("")
                        st.write("")
                        save_feedback_button = st.button(
                            "💾 Save Feedback",
                            key=f"save_feedback_{drill_type}",
                            use_container_width=True
                        )
                    
                    if save_feedback_button:
                        with st.spinner("Saving coach feedback to MongoDB..."):
                            success = save_coach_feedback_to_mongo(
                                st.session_state.assessment_id,
                                drill_type,
                                coach_feedback_early
                            )
                            if success:
                                st.session_state.saved_coach_feedback[feedback_key] = coach_feedback_early
                                st.success("✅ Coach feedback saved to MongoDB!")
                            else:
                                st.error("❌ Failed to save coach feedback")
                    
                    if st.session_state.saved_coach_feedback.get(feedback_key, ''):
                        st.info(f"💾 Saved feedback: {st.session_state.saved_coach_feedback[feedback_key][:100]}...")
                    
                    st.divider()
                    
                    if drill_type not in st.session_state.selected_clips:
                        st.session_state.selected_clips[drill_type] = []
                    
                    clips_to_show = st.session_state.processed_clips[:18]
                    
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
                    
                    if len(st.session_state.processed_clips) > 18:
                        st.info(f"Showing first 18 of {len(st.session_state.processed_clips)} clips")
                    
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
                    
                    # Check for backfoot defense clips if this is backfoot_drive
                    if drill_type == 'backfoot_drive' and drill_type in st.session_state.signed_urls:
                        has_defense = 'backfoot_defense' in st.session_state.drill_data
                        
                        if has_defense:
                            st.divider()
                            st.subheader("🛡️ Backfoot Defense Clips")
                            st.info("ℹ️ Backfoot defense clips are available. You can optionally select them to include in the task.")
                            
                            # Load defense clips if not already loaded
                            defense_key = f"{st.session_state.assessment_id}_backfoot_defense"
                            if defense_key not in st.session_state.defense_processed_clips:
                                load_defense = st.button("📥 Load Defense Clips", key="load_defense_clips")
                                
                                if load_defense:
                                    with st.spinner("Loading defense clips..."):
                                        defense_drill_info = st.session_state.drill_data['backfoot_defense']
                                        defense_clips, error = download_and_process_clips(
                                            st.session_state.assessment_id,
                                            st.session_state.player_name,
                                            'backfoot_defense',
                                            defense_drill_info['clips']
                                        )
                                        
                                        if error:
                                            st.error(error)
                                        else:
                                            st.session_state.defense_processed_clips[defense_key] = defense_clips
                                            st.rerun()
                            
                            # Display defense clips if loaded
                            if defense_key in st.session_state.defense_processed_clips:
                                defense_clips = st.session_state.defense_processed_clips[defense_key]
                                st.write(f"**Defense Clips Available:** {len(defense_clips)}")
                                
                                # Initialize selected defense clips
                                if 'backfoot_defense' not in st.session_state.selected_clips:
                                    st.session_state.selected_clips['backfoot_defense'] = []
                                
                                defense_clips_to_show = defense_clips[:18]
                                
                                for row in range(0, len(defense_clips_to_show), 3):
                                    cols = st.columns(3)
                                    
                                    for col_idx, clip_data in enumerate(defense_clips_to_show[row:row+3]):
                                        clip_idx = row + col_idx
                                        with cols[col_idx]:
                                            is_selected = st.checkbox(
                                                f"Select Defense Clip {clip_idx + 1}",
                                                value=clip_idx in st.session_state.selected_clips['backfoot_defense'],
                                                key=f"select_defense_{clip_idx}"
                                            )
                                            
                                            if is_selected and clip_idx not in st.session_state.selected_clips['backfoot_defense']:
                                                st.session_state.selected_clips['backfoot_defense'].append(clip_idx)
                                            elif not is_selected and clip_idx in st.session_state.selected_clips['backfoot_defense']:
                                                st.session_state.selected_clips['backfoot_defense'].remove(clip_idx)
                                            
                                            st.write(f"**Defense Clip {clip_idx + 1}**")
                                            st.video(clip_data['bytes'])
                                
                                if len(defense_clips) > 18:
                                    st.info(f"Showing first 18 of {len(defense_clips)} defense clips")
                                
                                # Upload defense clips
                                num_defense_selected = len(st.session_state.selected_clips.get('backfoot_defense', []))
                                st.write(f"**Selected Defense Clips:** {num_defense_selected}")
                                
                                if num_defense_selected > 0:
                                    col1, col2 = st.columns([1, 3])
                                    with col1:
                                        upload_defense_button = st.button(
                                            "☁️ Upload Defense to GCS",
                                            key="upload_defense",
                                            use_container_width=True,
                                            disabled=drill_type in st.session_state.defense_signed_urls
                                        )
                                    
                                    if upload_defense_button:
                                        defense_urls, error = upload_selected_clips_to_gcs(
                                            st.session_state.assessment_id,
                                            st.session_state.player_name,
                                            'backfoot_defense',
                                            st.session_state.selected_clips['backfoot_defense'],
                                            defense_clips
                                        )
                                        
                                        if error:
                                            st.error(error)
                                        else:
                                            st.session_state.defense_signed_urls[drill_type] = defense_urls
                                            st.success(f"✅ Uploaded {len(defense_urls)} defense clips to GCS!")
                                    
                                    if drill_type in st.session_state.defense_signed_urls:
                                        st.success(f"✅ {len(st.session_state.defense_signed_urls[drill_type])} defense clips uploaded to GCS")
                    
                    if drill_type in st.session_state.signed_urls:
                        st.success(f"✅ {len(st.session_state.signed_urls[drill_type])} clips uploaded to GCS")
                        
                        # Auto-populate coach feedback from saved feedback
                        feedback_key = f"{st.session_state.assessment_id}_{drill_type}"
                        default_feedback = st.session_state.saved_coach_feedback.get(feedback_key, '')
                        
                        coach_feedback = st.text_area(
                            "Coach Feedback (optional)",
                            value=default_feedback,
                            key=f"feedback_{drill_type}",
                            placeholder="Enter any coach feedback here..."
                        )
                        
                        if default_feedback:
                            st.info("ℹ️ Coach feedback auto-populated from saved feedback")
                        
                        # Get defense URLs if available for backfoot_drive
                        defense_urls = []
                        if drill_type == 'backfoot_drive' and drill_type in st.session_state.defense_signed_urls:
                            defense_urls = st.session_state.defense_signed_urls[drill_type]
                        
                        flyte_inputs = build_flyte_inputs(
                            drill_type,
                            st.session_state.player_name,
                            st.session_state.assessment_id,
                            st.session_state.signed_urls[drill_type],
                            coach_feedback,
                            defense_urls
                        )
                        
                        st.write("**Flyte Task Inputs:**")
                        if drill_type == 'backfoot_drive':
                            if defense_urls:
                                st.info(f"ℹ️ Task includes {len(st.session_state.signed_urls[drill_type])} drive clips and {len(defense_urls)} defense clips")
                            else:
                                st.info(f"ℹ️ Task includes {len(st.session_state.signed_urls[drill_type])} drive clips (no defense clips)")
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
                                
                                # Show metrics editor for drive drills when task is completed
                                if is_done and drill_type in ['tophand', 'bottomhand', 'backfoot_drive', 'backfoot_defense']:
                                    st.divider()
                                    st.subheader("✏️ Edit Metrics")
                                    
                                    # Fetch drill results from MongoDB
                                    metrics_key = f"{st.session_state.assessment_id}_{drill_type}"
                                    
                                    if metrics_key not in st.session_state.drill_metrics_data:
                                        with st.spinner("Loading drill metrics from MongoDB..."):
                                            drill_doc = fetch_drill_results(st.session_state.assessment_id)
                                            
                                            if drill_doc and 'drill_metrics' in drill_doc:
                                                drill_metrics = drill_doc['drill_metrics']
                                                
                                                # Show available drills for debugging
                                                available_drills = list(drill_metrics.keys())
                                                st.info(f"📋 Available drills in MongoDB: {', '.join(available_drills)}")
                                                
                                                # Get MongoDB drill key using the mapping function
                                                mongo_drill_key = get_mongo_drill_key(drill_type)
                                                
                                                # Also try alternate keys
                                                possible_keys = [mongo_drill_key, drill_type]
                                                found_key = None
                                                
                                                for key in possible_keys:
                                                    if key in drill_metrics:
                                                        found_key = key
                                                        break
                                                
                                                if found_key:
                                                    st.session_state.drill_metrics_data[metrics_key] = drill_metrics[found_key]
                                                    st.session_state.mongo_drill_keys[metrics_key] = found_key
                                                    
                                                    # Initialize edited values
                                                    metrics = drill_metrics[found_key]
                                                    shot_angles = metrics.get('shot_direction_angles', [])
                                                    ball_dirs = metrics.get('ball_directions', [])
                                                    
                                                    if not shot_angles or not ball_dirs:
                                                        st.error(f"⚠️ Drill '{found_key}' exists but has no shot_direction_angles or ball_directions data")
                                                    else:
                                                        st.session_state.edited_angles[metrics_key] = shot_angles.copy()
                                                        st.session_state.edited_directions[metrics_key] = ball_dirs.copy()
                                                        st.session_state.original_values[metrics_key] = {
                                                            'angles': shot_angles.copy(),
                                                            'directions': ball_dirs.copy()
                                                        }
                                                else:
                                                    st.warning(f"❌ No metrics found for '{drill_type}' (tried keys: {', '.join(possible_keys)}) in MongoDB")
                                                    st.write("**Available drill types to select from:**")
                                                    for avail_drill in available_drills:
                                                        st.write(f"- {avail_drill}")
                                            else:
                                                st.warning("No drill results found in MongoDB for this assessment")
                                    
                                    if metrics_key in st.session_state.drill_metrics_data:
                                        metrics = st.session_state.drill_metrics_data[metrics_key]
                                        
                                        # Get the actual MongoDB key that was found
                                        mongo_drill_key = st.session_state.mongo_drill_keys.get(metrics_key, drill_type)
                                        
                                        # Get batter hand for calculations
                                        batter_hand = 'Right'
                                        if 'new_metrics' in metrics and 'batter_hand' in metrics['new_metrics']:
                                            batter_hands = metrics['new_metrics']['batter_hand']
                                            if batter_hands:
                                                batter_hand = max(set(batter_hands), key=batter_hands.count)
                                        
                                        st.write(f"**Batter Hand:** {batter_hand}")
                                        st.write(f"**MongoDB Drill Key:** `{mongo_drill_key}`")
                                        
                                        # Ball direction options
                                        ball_direction_options = ['Off Stump', 'Outside Off Stump', 'Middle Stump', 'Leg Stump']
                                        
                                        num_shots = len(st.session_state.edited_angles[metrics_key])
                                        st.write(f"**Number of Shots:** {num_shots}")
                                        
                                        # Create editable fields in a table-like format
                                        st.write("**Edit Shot Direction Angles and Ball Directions:**")
                                        
                                        # Header row
                                        col1, col2, col3 = st.columns([1, 2, 2])
                                        with col1:
                                            st.write("**Shot #**")
                                        with col2:
                                            st.write("**Shot Direction Angle**")
                                        with col3:
                                            st.write("**Ball Direction**")
                                        
                                        for i in range(num_shots):
                                            col1, col2, col3 = st.columns([1, 2, 2])
                                            
                                            with col1:
                                                st.write(f"Shot {i+1}")
                                            
                                            with col2:
                                                current_angle = st.session_state.edited_angles[metrics_key][i]
                                                new_angle = st.number_input(
                                                    f"Angle {i+1}",
                                                    value=float(current_angle),
                                                    step=1.0,
                                                    key=f"angle_{drill_type}_{i}",
                                                    label_visibility="collapsed"
                                                )
                                                st.session_state.edited_angles[metrics_key][i] = new_angle
                                            
                                            with col3:
                                                current_direction = st.session_state.edited_directions[metrics_key][i]
                                                direction_index = ball_direction_options.index(current_direction) if current_direction in ball_direction_options else 0
                                                new_direction = st.selectbox(
                                                    f"Ball Direction {i+1}",
                                                    options=ball_direction_options,
                                                    index=direction_index,
                                                    key=f"direction_{drill_type}_{i}",
                                                    label_visibility="collapsed"
                                                )
                                                st.session_state.edited_directions[metrics_key][i] = new_direction
                                        
                                        # Check if values have changed
                                        angles_changed = st.session_state.edited_angles[metrics_key] != st.session_state.original_values[metrics_key]['angles']
                                        directions_changed = st.session_state.edited_directions[metrics_key] != st.session_state.original_values[metrics_key]['directions']
                                        values_changed = angles_changed or directions_changed
                                        
                                        # Update button
                                        col1, col2, col3 = st.columns([1, 2, 2])
                                        with col1:
                                            update_button = st.button(
                                                "💾 Update Metrics",
                                                key=f"update_{drill_type}",
                                                disabled=not values_changed,
                                                use_container_width=True
                                            )
                                        
                                        if values_changed:
                                            st.info("⚠️ You have unsaved changes")
                                        
                                        if update_button:
                                            with st.spinner("Updating metrics..."):
                                                try:
                                                    # Recalculate all metrics
                                                    updated_metrics = recalculate_metrics(
                                                        metrics,
                                                        st.session_state.edited_angles[metrics_key],
                                                        st.session_state.edited_directions[metrics_key],
                                                        batter_hand
                                                    )
                                                    
                                                    # Update in MongoDB using the correct key
                                                    success = update_drill_results_in_mongo(
                                                        st.session_state.assessment_id,
                                                        mongo_drill_key,
                                                        updated_metrics
                                                    )
                                                    
                                                    if success:
                                                        st.success("✅ Metrics updated successfully in MongoDB!")
                                                        
                                                        # Update session state
                                                        st.session_state.drill_metrics_data[metrics_key] = updated_metrics
                                                        st.session_state.original_values[metrics_key] = {
                                                            'angles': st.session_state.edited_angles[metrics_key].copy(),
                                                            'directions': st.session_state.edited_directions[metrics_key].copy()
                                                        }
                                                        
                                                        # Display updated metrics
                                                        st.divider()
                                                        st.subheader("📊 Updated Metrics")
                                                        
                                                        with st.expander("View Updated MongoDB Document", expanded=True):
                                                            st.json(updated_metrics)
                                                        
                                                        # Display key metrics
                                                        col1, col2, col3 = st.columns(3)
                                                        with col1:
                                                            st.metric("Grade", updated_metrics.get('grade', 'N/A'))
                                                        with col2:
                                                            st.metric("Percentile", f"{updated_metrics.get('percentile', 0)}%")
                                                        with col3:
                                                            if 'new_metrics' in updated_metrics and 'score' in updated_metrics['new_metrics']:
                                                                avg_score = sum(updated_metrics['new_metrics']['score']) / len(updated_metrics['new_metrics']['score'])
                                                                st.metric("Avg Score", f"{avg_score:.2f}")
                                                        
                                                        st.rerun()
                                                    else:
                                                        st.error("❌ Failed to update metrics in MongoDB")
                                                
                                                except Exception as e:
                                                    st.error(f"Error updating metrics: {str(e)}")
                                                    st.exception(e)
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
    5. Add and save coach feedback (before clip selection)
    6. Select clips for Flyte tasks
    7. Upload clips to GCS
    8. Run Flyte tasks with custom inputs (auto-populates saved feedback)
    9. Monitor task execution status
    10. Edit shot angles and ball directions (drive drills)
    11. Auto-recalculate metrics and save to MongoDB
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
        st.session_state.drill_metrics_data = {}
        st.session_state.edited_angles = {}
        st.session_state.edited_directions = {}
        st.session_state.original_values = {}
        st.session_state.mongo_drill_keys = {}
        st.session_state.defense_processed_clips = {}
        st.session_state.defense_signed_urls = {}
        st.session_state.saved_coach_feedback = {}
        st.success("Cache cleared!")
        st.rerun()








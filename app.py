import streamlit as st
from pymongo import MongoClient
import os
import tempfile
from moviepy import VideoFileClip
from google.cloud import storage
from datetime import timedelta
import json
import time
import requests
from typing import Dict, Any
from dotenv import load_dotenv
import cv2
import numpy as np
from PIL import Image
import io
from streamlit_drawable_canvas import st_canvas

load_dotenv()



# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

GCS_BUCKET_NAME = "qr-ai"
GCS_PARENT_FOLDER_NAME = "cricket_batting/"
SERVICE_ACCOUNT_FILE = st.secrets["SERVICE_ACCOUNT_FILE"]

BASE_URL = st.secrets["FLYTE_API_BASE_URL"]
API_KEY = os.getenv("FLYTE_API_KEY")

# MongoDB connection
connection_string = st.secrets["MONGO_CONNECTION_STRING"]
client = MongoClient(connection_string)
db = client['kpro']
gallery_collection = db['gallery']
users_collection = db['users']

# MongoDB staging connection for drill results
staging_connection_string = st.secrets["MONGO_CONNECTION_STRING_STAGING"]
staging_client = MongoClient(staging_connection_string)
staging_db = staging_client['kpro']
drills_collection = staging_db['drill_results']


# ============================================================================
# DIAGNOSTIC CHECKS FOR MOVIEPY
# ============================================================================
def check_video_dependencies():
    """Check if video processing dependencies are available"""
    issues = []
    try:
        from moviepy import VideoFileClip
        print(f"✅ MoviePy 2.x loaded successfully")
    except Exception as e:
        issues.append(f"Error loading MoviePy: {str(e)}")
    
    return issues

# Run diagnostic check
DEPENDENCY_ISSUES = check_video_dependencies()
if DEPENDENCY_ISSUES:
    print("⚠️ Video processing dependency issues detected:")
    for issue in DEPENDENCY_ISSUES:
        print(f"  - {issue}")


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
    Uploads a file to GCS. If a file with the same name exists, it will be replaced.
    Returns the public URL or (signed_url, public_url) if signed_url_flag=True.

    Args:
        bucket_name (str): GCS bucket name.
        source_file_name (str): Local path to file.
        destination_blob_name (str): Blob name in GCS.
        signed_url_flag (bool): If True, also return a signed url.

    Returns:
        str or tuple: Public url, or (signed_url, public_url) if signed_url_flag=True.
    """
    try:
        storage_client = get_gcs_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Always upload/replace with the new file.
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded (and replaced if existed) to gs://{bucket_name}/{destination_blob_name}.")

        public_url = blob.public_url

        if signed_url_flag:
            try:
                signed_url = blob.generate_signed_url(expiration=timedelta(days=7))
                print(f"Signed URL: {signed_url}")
                return signed_url, public_url
            except Exception as e:
                print(f"Could not generate signed URL: {e}")
                return public_url
        else:
            print(f"Public URL: {public_url}")
            return public_url

    except Exception as e:
        print(f"Error uploading/replacing file to GCS: {e}")
        return None


def upload_to_gcs_without_replace(bucket_name, source_file_name, destination_blob_name, signed_url_flag=False):
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
    temp_input_path = None
    temp_output_path = None
    clip = None
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_input:
            temp_input.write(video_bytes)
            temp_input_path = temp_input.name
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_output:
            temp_output_path = temp_output.name
        
        clip = VideoFileClip(temp_input_path)
        clip.write_videofile(temp_output_path, codec='libx264', audio=True, logger=None)
        clip.close()
        clip = None
        
        # Small delay to ensure file handles are released on Windows
        time.sleep(0.1)
        
        with open(temp_output_path, 'rb') as f:
            processed_bytes = f.read()
        
        return processed_bytes
        
    except Exception as e:
        print(f"Error processing clip {clip_name}: {e}")
        return None
    
    finally:
        # Ensure clip is closed
        try:
            if clip is not None:
                clip.close()
        except:
            pass
        
        # Small delay before cleanup
        time.sleep(0.1)
        
        # Clean up temp files
        if temp_input_path and os.path.exists(temp_input_path):
            try:
                os.unlink(temp_input_path)
            except Exception as e:
                print(f"Warning: Could not delete temp input file: {e}")
        
        if temp_output_path and os.path.exists(temp_output_path):
            try:
                os.unlink(temp_output_path)
            except Exception as e:
                print(f"Warning: Could not delete temp output file: {e}")

def create_clip_from_video(video_bytes, start_time, end_time, clip_name, blur_mask=None, blur_level=1):
    """Create a clip from video bytes using start and end times (in seconds)
    Returns: tuple (clip_bytes, error_message) - clip_bytes is None if error occurred
    """
    temp_input_path = None
    temp_output_path = None
    video = None
    clip = None
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_input:
            temp_input.write(video_bytes)
            temp_input_path = temp_input.name
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_output:
            temp_output_path = temp_output.name
        
        video = VideoFileClip(temp_input_path)
        
        # Validate times
        if start_time < 0:
            start_time = 0
        if end_time > video.duration:
            end_time = video.duration
        if start_time >= end_time:
            video.close()
            error_msg = f"Start time ({start_time}) must be less than end time ({end_time})"
            print(f"Error creating clip {clip_name}: {error_msg}")
            return None, error_msg
        
        clip = video.subclipped(start_time, end_time)
        
        # If blur mask is provided, apply blur using OpenCV approach
        if blur_mask is not None and blur_level > 0:
            # Apply blur frame by frame
            def blur_frame(get_frame, t):
                frame = get_frame(t)
                return apply_blur_to_frame(frame, blur_mask, blur_level)
            
            clip = clip.transform(blur_frame)
        
        try:
            clip.write_videofile(
                temp_output_path, 
                codec='libx264',
                audio_codec='aac',
                logger=None,
                preset='ultrafast',
                threads=4
            )
        except Exception as codec_error:
            print(f"Failed with standard codec, trying without audio: {codec_error}")
            clip.write_videofile(
                temp_output_path,
                codec='libx264',
                audio=False,
                logger=None,
                preset='ultrafast',
                threads=4
            )
        
        # Close video objects before reading output
        clip.close()
        video.close()
        clip = None
        video = None
        
        # Small delay to ensure file handles are released on Windows
        time.sleep(0.1)
        
        with open(temp_output_path, 'rb') as f:
            clip_bytes = f.read()
        
        return clip_bytes, None
        
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"Error creating clip {clip_name}: {error_msg}")
        import traceback
        traceback_str = traceback.format_exc()
        print(traceback_str)
        
        # Add more context to error message
        if "FFMPEG" in str(e).upper():
            error_msg += " (Hint: FFMPEG binary issue detected)"
        
        return None, error_msg
    
    finally:
        # Ensure all video objects are closed
        try:
            if clip is not None:
                clip.close()
        except:
            pass
        
        try:
            if video is not None:
                video.close()
        except:
            pass
        
        # Small delay before cleanup
        time.sleep(0.1)
        
        # Clean up temp files
        if temp_input_path and os.path.exists(temp_input_path):
            try:
                os.unlink(temp_input_path)
            except Exception as e:
                print(f"Warning: Could not delete temp input file: {e}")
        
        if temp_output_path and os.path.exists(temp_output_path):
            try:
                os.unlink(temp_output_path)
            except Exception as e:
                print(f"Warning: Could not delete temp output file: {e}")

def get_video_duration(video_bytes):
    """Get duration of video in seconds"""
    temp_input_path = None
    video = None
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_input:
            temp_input.write(video_bytes)
            temp_input_path = temp_input.name
        
        video = VideoFileClip(temp_input_path)
        duration = video.duration
        video.close()
        video = None
        
        # Small delay to ensure file handles are released on Windows
        time.sleep(0.1)
        
        return duration
        
    except Exception as e:
        print(f"Error getting video duration: {e}")
        return None
    
    finally:
        # Ensure video is closed
        try:
            if video is not None:
                video.close()
        except:
            pass
        
        # Small delay before cleanup
        time.sleep(0.1)
        
        # Clean up temp file
        if temp_input_path and os.path.exists(temp_input_path):
            try:
                os.unlink(temp_input_path)
            except Exception as e:
                print(f"Warning: Could not delete temp file: {e}")

def get_video_first_frame(video_bytes):
    """Extract first frame from video for blur mask creation"""
    temp_input_path = None
    cap = None
    
    try:
        # Validate input
        if not video_bytes or len(video_bytes) == 0:
            print("Error: Empty video_bytes provided")
            return None
        
        print(f"Video bytes size: {len(video_bytes)} bytes")
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False, mode='wb') as temp_input:
            temp_input.write(video_bytes)
            temp_input_path = temp_input.name
        
        print(f"Temp file created: {temp_input_path}")
        print(f"Temp file size: {os.path.getsize(temp_input_path)} bytes")
        
        # Verify OpenCV is available
        if not hasattr(cv2, 'VideoCapture'):
            print("Error: cv2.VideoCapture not available")
            return None
        
        # Use OpenCV to read first frame
        cap = cv2.VideoCapture(temp_input_path)
        
        if not cap.isOpened():
            print("Error: Could not open video file with OpenCV")
            # Try alternative approach using MoviePy
            try:
                print("Attempting fallback with MoviePy...")
                video = VideoFileClip(temp_input_path)
                frame = video.get_frame(0)
                video.close()
                print("Successfully extracted frame using MoviePy")
                return frame
            except Exception as moviepy_error:
                print(f"MoviePy fallback failed: {moviepy_error}")
                return None
        
        ret, frame = cap.read()
        
        if ret and frame is not None:
            print(f"Frame extracted successfully: shape={frame.shape}")
            # Convert BGR to RGB
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            return frame_rgb
        else:
            print("Error: Could not read frame from video")
            return None
            
    except Exception as e:
        print(f"Error extracting first frame: {type(e).__name__}: {e}")
        import traceback
        print(traceback.format_exc())
        return None
    
    finally:
        # Release video capture
        if cap is not None:
            try:
                cap.release()
            except:
                pass
        
        # Clean up temp file
        time.sleep(0.1)
        if temp_input_path and os.path.exists(temp_input_path):
            try:
                os.unlink(temp_input_path)
            except Exception as e:
                print(f"Warning: Could not delete temp file: {e}")

def apply_blur_to_frame(frame, blur_mask, blur_level):
    """Apply blur to a single frame using the blur mask"""
    try:
        if blur_mask is None or blur_level == 0:
            return frame
        
        # Resize mask to match frame size if needed
        if blur_mask.shape[:2] != frame.shape[:2]:
            blur_mask = cv2.resize(blur_mask, (frame.shape[1], frame.shape[0]), interpolation=cv2.INTER_NEAREST)
        
        # Convert mask to binary
        mask_binary = (blur_mask > 0).astype(np.uint8)
        
        # Determine blur kernel size based on level (1=low, 2=medium, 3=high)
        kernel_sizes = {1: 15, 2: 31, 3: 51}
        kernel_size = kernel_sizes.get(blur_level, 31)
        
        # Ensure kernel size is odd
        if kernel_size % 2 == 0:
            kernel_size += 1
        
        # Apply Gaussian blur to the entire frame
        blurred_frame = cv2.GaussianBlur(frame, (kernel_size, kernel_size), 0)
        
        # Expand mask to 3 channels
        mask_3channel = np.stack([mask_binary] * 3, axis=-1)
        
        # Combine original and blurred frames using mask
        result = np.where(mask_3channel, blurred_frame, frame)
        
        return result.astype(np.uint8)
        
    except Exception as e:
        print(f"Error applying blur to frame: {e}")
        return frame

def apply_blur_to_video(video_bytes, blur_mask, blur_level, progress_callback=None):
    """Apply blur mask to entire video"""
    temp_input_path = None
    temp_output_path = None
    
    try:
        # Write video bytes to temp file
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_input:
            temp_input.write(video_bytes)
            temp_input_path = temp_input.name
        
        # Open video with OpenCV
        cap = cv2.VideoCapture(temp_input_path)
        
        # Get video properties
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        # Create temp output file
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_output:
            temp_output_path = temp_output.name
        
        # Create video writer
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(temp_output_path, fourcc, fps, (width, height))
        
        frame_count = 0
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Apply blur to frame
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            blurred_frame_rgb = apply_blur_to_frame(frame_rgb, blur_mask, blur_level)
            blurred_frame_bgr = cv2.cvtColor(blurred_frame_rgb, cv2.COLOR_RGB2BGR)
            
            out.write(blurred_frame_bgr)
            
            frame_count += 1
            if progress_callback and total_frames > 0:
                progress_callback(frame_count / total_frames)
        
        cap.release()
        out.release()
        
        # Small delay to ensure file handles are released
        time.sleep(0.1)
        
        # Read processed video
        with open(temp_output_path, 'rb') as f:
            processed_video_bytes = f.read()
        
        return processed_video_bytes
        
    except Exception as e:
        print(f"Error applying blur to video: {e}")
        return None
    
    finally:
        # Clean up temp files
        time.sleep(0.1)
        if temp_input_path and os.path.exists(temp_input_path):
            try:
                os.unlink(temp_input_path)
            except Exception as e:
                print(f"Warning: Could not delete temp input file: {e}")
        
        if temp_output_path and os.path.exists(temp_output_path):
            try:
                os.unlink(temp_output_path)
            except Exception as e:
                print(f"Warning: Could not delete temp output file: {e}")

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


    # metrics['insights'] = score_metrics.get('insights', {})
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
        result = drills_collection.update_one(search_query, update_query, upsert=False)
        print(f"MongoDB update — matched: {result.matched_count}, modified: {result.modified_count}, assessment_id: {assessment_id}, drill: {drill_type}")
        if result.matched_count == 0:
            print(f"⚠️ No document found with assessment_id='{assessment_id}' in drills collection")
            return False
        # matched_count > 0 means the document was found and the $set was applied
        # (modified_count can be 0 if the new value is identical to the stored value)
        return True
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
            # Only process records with status 'processed' or 'uploaded'
            doc_status = doc.get('status', '')
            if doc_status not in ['processed', 'uploaded']:
                continue
            
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
            else:
                # Add drill even if it has 0 clips (for manual clipping support)
                drill_data[drill_type] = {
                    'title': drill,
                    'clips': clips if clips else [],
                    'drill_id': drill_id,
                    'activity_id': activity_id,
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
        player_name = player_name.replace('.', '_')
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
        player_name_formatted = player_name_formatted.replace('.', '_')
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
    player_name_formatted = player_name_formatted.replace('.', '_')
    
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
    st.session_state.processed_clips = {}
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
if 'manual_clipping_mode' not in st.session_state:
    st.session_state.manual_clipping_mode = {}
if 'raw_video_data' not in st.session_state:
    st.session_state.raw_video_data = {}
if 'num_manual_clips' not in st.session_state:
    st.session_state.num_manual_clips = {}
if 'manual_clip_times' not in st.session_state:
    st.session_state.manual_clip_times = {}
if 'manual_clips_created' not in st.session_state:
    st.session_state.manual_clips_created = {}
if 'clip_selection_mode' not in st.session_state:
    st.session_state.clip_selection_mode = {}
if 'auto_clips_cache' not in st.session_state:
    st.session_state.auto_clips_cache = {}
if 'manual_clips_cache' not in st.session_state:
    st.session_state.manual_clips_cache = {}
if 'blur_masks' not in st.session_state:
    st.session_state.blur_masks = {}
if 'blur_levels' not in st.session_state:
    st.session_state.blur_levels = {}
if 'blur_history' not in st.session_state:
    st.session_state.blur_history = {}
if 'blur_enabled' not in st.session_state:
    st.session_state.blur_enabled = {}

# --- Manual Generation session state ---
if 'mg_raw_video_data' not in st.session_state:
    st.session_state.mg_raw_video_data = {}
if 'mg_num_manual_clips' not in st.session_state:
    st.session_state.mg_num_manual_clips = {}
if 'mg_manual_clip_times' not in st.session_state:
    st.session_state.mg_manual_clip_times = {}
if 'mg_manual_clips_created' not in st.session_state:
    st.session_state.mg_manual_clips_created = {}
if 'mg_processed_clips' not in st.session_state:
    st.session_state.mg_processed_clips = {}
if 'mg_blur_masks' not in st.session_state:
    st.session_state.mg_blur_masks = {}
if 'mg_blur_levels' not in st.session_state:
    st.session_state.mg_blur_levels = {}
if 'mg_blur_history' not in st.session_state:
    st.session_state.mg_blur_history = {}
if 'mg_blur_enabled' not in st.session_state:
    st.session_state.mg_blur_enabled = {}
if 'mg_signed_urls' not in st.session_state:
    st.session_state.mg_signed_urls = {}
if 'mg_execution_ids' not in st.session_state:
    st.session_state.mg_execution_ids = {}
if 'mg_task_status' not in st.session_state:
    st.session_state.mg_task_status = {}
if 'mg_saved_coach_feedback' not in st.session_state:
    st.session_state.mg_saved_coach_feedback = {}
if 'mg_running_drill_inputs' not in st.session_state:
    st.session_state.mg_running_drill_inputs = {}
if 'mg_assessment_started' not in st.session_state:
    st.session_state.mg_assessment_started = False
if 'mg_assessment_id_val' not in st.session_state:
    st.session_state.mg_assessment_id_val = ''
if 'mg_player_name_val' not in st.session_state:
    st.session_state.mg_player_name_val = ''
if 'mg_drill_metrics_data' not in st.session_state:
    st.session_state.mg_drill_metrics_data = {}
if 'mg_edited_angles' not in st.session_state:
    st.session_state.mg_edited_angles = {}
if 'mg_edited_directions' not in st.session_state:
    st.session_state.mg_edited_directions = {}
if 'mg_original_values' not in st.session_state:
    st.session_state.mg_original_values = {}
if 'mg_mongo_drill_keys' not in st.session_state:
    st.session_state.mg_mongo_drill_keys = {}

# ============================================================================
# STREAMLIT APP UI
# ============================================================================

st.set_page_config(page_title="Cricket Batting Drills Viewer", layout="wide")
st.title("🏏 Cricket Batting Drills Viewer")

tab_search, tab_manual = st.tabs(["🔍 Search Assessment", "🎬 Manual Generation"])

# ============================================================================
# TAB 1 — SEARCH ASSESSMENT (existing flow)
# ============================================================================
with tab_search:
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
            st.session_state.processed_clips = {}
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
            st.session_state.manual_clipping_mode = {}
            st.session_state.raw_video_data = {}
            st.session_state.num_manual_clips = {}
            st.session_state.manual_clip_times = {}
            st.session_state.manual_clips_created = {}
            st.session_state.clip_selection_mode = {}
            st.session_state.auto_clips_cache = {}
            st.session_state.manual_clips_cache = {}
            st.session_state.blur_masks = {}
            st.session_state.blur_levels = {}
            st.session_state.blur_history = {}
            st.session_state.blur_enabled = {}
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
                        # Don't reset processed_clips - each drill has its own entry in the dict
                        # Initialize clip selection mode if not exists
                        if drill_type not in st.session_state.clip_selection_mode:
                            st.session_state.clip_selection_mode[drill_type] = None
            
            if st.session_state.selected_drill:
                st.divider()
                drill_type = st.session_state.selected_drill
                drill_info = st.session_state.drill_data[drill_type]
                
                st.subheader(f"Drill: {drill_info['title']}")
                
                # Clip Selection Mode Buttons (only for multi-clip drills)
                if not drill_info.get('is_single_video', False):
                    st.divider()
                    
                    # Check if drill has any clips
                    num_clips = len(drill_info.get('clips', []))
                    has_clips = num_clips > 0
                    
                    # Get current mode
                    current_mode = st.session_state.clip_selection_mode.get(drill_type, None)
                    
                    # Auto-select manual mode if no clips available
                    if not has_clips and not current_mode:
                        st.session_state.clip_selection_mode[drill_type] = 'manual'
                        st.session_state.manual_clipping_mode[drill_type] = True
                        current_mode = 'manual'
                    
                    # Show info if no clips available
                    if not has_clips:
                        st.warning("⚠️ No auto-generated clips available for this drill. Only **Manual Clipping** mode is enabled.")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        manual_clip_button = st.button(
                            "✂️ Manual Clipping",
                            key=f"manual_clip_{drill_type}",
                            use_container_width=True,
                            disabled=(current_mode == 'auto' and has_clips),
                            type="primary" if current_mode == 'manual' else "secondary"
                        )
                    
                    with col2:
                        auto_clip_button = st.button(
                            "📹 Select from Auto Generated Clips",
                            key=f"auto_clip_{drill_type}",
                            use_container_width=True,
                            disabled=(current_mode == 'manual') or not has_clips,
                            type="primary" if current_mode == 'auto' else "secondary"
                        )
                    
                    if manual_clip_button:
                        st.session_state.clip_selection_mode[drill_type] = 'manual'
                        st.session_state.manual_clipping_mode[drill_type] = True
                        st.session_state.processed_clips[drill_type] = []
                        st.session_state.manual_clips_created[drill_type] = False
                    
                    if auto_clip_button and has_clips:
                        st.session_state.clip_selection_mode[drill_type] = 'auto'
                        st.session_state.manual_clipping_mode[drill_type] = False
                        st.session_state.processed_clips[drill_type] = []
                    
                    # Show message if no mode selected (and clips are available)
                    if not current_mode and has_clips:
                        st.info("👆 Please select a clip mode: **Manual Clipping** or **Auto Generated Clips**")
                    
                    # Check if we're in manual clipping mode
                    if st.session_state.clip_selection_mode.get(drill_type) == 'manual':
                        st.info("🎬 Manual Clipping Mode: Create custom clips from raw video")
                        
                        # Show dependency warnings if any
                        if DEPENDENCY_ISSUES:
                            st.warning("⚠️ **Video Processing Dependencies Issue Detected**")
                            with st.expander("View Dependency Issues (Click to expand)"):
                                st.write("The following issues may prevent video clipping from working:")
                                for issue in DEPENDENCY_ISSUES:
                                    st.write(f"• {issue}")
                                st.write("\n**Note:** Ensure MoviePy and its dependencies are properly installed.")
                        
                        # Get drill_id and activity_id from gallery collection
                        drill_id = drill_info.get('drill_id', '')
                        activity_id = drill_info.get('activity_id', '')
                        
                        if not drill_id or not activity_id:
                            # Try to fetch from MongoDB gallery collection
                            # Only use records with status 'processed' or 'uploaded'
                            try:
                                gallery_query = {
                                    "user_id": st.session_state.assessment_id,
                                    "title": {"$regex": drill_type, "$options": "i"},
                                    "status": {"$in": ["processed", "uploaded"]}
                                }
                                gallery_doc = gallery_collection.find_one(gallery_query)
                                if gallery_doc:
                                    drill_id = gallery_doc.get('drill_id', '')
                                    activity_id = gallery_doc.get('activity_id', '')
                                    st.info(f"✅ Found drill record with status: {gallery_doc.get('status', 'N/A')}")
                                else:
                                    # If no processed/uploaded record found, show available records
                                    all_records_query = {
                                        "user_id": st.session_state.assessment_id,
                                        "title": {"$regex": drill_type, "$options": "i"}
                                    }
                                    all_records = list(gallery_collection.find(all_records_query))
                                    if all_records:
                                        statuses = [rec.get('status', 'unknown') for rec in all_records]
                                        st.warning(f"⚠️ No 'processed' or 'uploaded' record found. Available statuses: {', '.join(statuses)}")
                            except Exception as e:
                                st.error(f"Error fetching drill info: {e}")
                        
                        if drill_id and activity_id:
                            raw_video_url = f"gs://uploads.storage.khiladipro.com/{drill_id}/{activity_id}/{activity_id}.mp4"
                            st.write(f"**Raw Video URL:** `{raw_video_url}`")
                            
                            # Check if manual clips already exist in cache
                            manual_cache_key = f"{st.session_state.assessment_id}_{drill_type}_manual"
                            if manual_cache_key in st.session_state.manual_clips_cache and drill_type not in st.session_state.processed_clips:
                                st.session_state.processed_clips[drill_type] = st.session_state.manual_clips_cache[manual_cache_key]
                                st.session_state.manual_clips_created[drill_type] = True
                                st.info("📦 Loaded manual clips from cache!")
                            
                            # Download raw video
                            raw_video_key = f"{st.session_state.assessment_id}_{drill_type}_raw"
                            if raw_video_key not in st.session_state.raw_video_data:
                                with st.spinner("Downloading raw video..."):
                                    try:
                                        bucket_name, video_object_name = parse_gsutil_url(raw_video_url)
                                        video_bytes = download_gcs_file_to_bytes(bucket_name, video_object_name)
                                        
                                        if video_bytes:
                                            # Get video duration
                                            duration = get_video_duration(video_bytes)
                                            st.session_state.raw_video_data[raw_video_key] = {
                                                'bytes': video_bytes,
                                                'duration': duration,
                                                'url': raw_video_url
                                            }
                                            st.success(f"✅ Raw video loaded! Duration: {duration:.2f} seconds")
                                        else:
                                            st.error("Failed to download raw video")
                                    except Exception as e:
                                        st.error(f"Error downloading raw video: {e}")
                            
                            # Display raw video and clip creation interface
                            if raw_video_key in st.session_state.raw_video_data:
                                raw_video_info = st.session_state.raw_video_data[raw_video_key]
                                video_duration = raw_video_info['duration']
                                
                                col_video, col_controls = st.columns([1, 3])
                                
                                with col_video:
                                    st.write("**Raw Video Preview:**")
                                    st.video(raw_video_info['bytes'])
                                    st.info(f"📹 Video Duration: {video_duration:.2f} seconds")
                                
                                with col_controls:
                                    st.write("**Clip Settings:**")
                                    
                                    # Number of clips selector
                                    if drill_type not in st.session_state.num_manual_clips:
                                        st.session_state.num_manual_clips[drill_type] = 1
                                    
                                    num_clips = st.number_input(
                                        "Number of clips (1-6)",
                                        min_value=1,
                                        max_value=6,
                                        value=st.session_state.num_manual_clips[drill_type],
                                        key=f"num_clips_{drill_type}"
                                    )
                                    st.session_state.num_manual_clips[drill_type] = num_clips
                                
                                # Blur Area Selection Section
                                st.divider()
                                st.subheader("🎨 Blur Area Selection (Optional)")
                                
                                blur_key = f"{st.session_state.assessment_id}_{drill_type}"
                                
                                # Initialize blur enabled state
                                if blur_key not in st.session_state.blur_enabled:
                                    st.session_state.blur_enabled[blur_key] = False
                                
                                enable_blur = st.checkbox(
                                    "Enable blur for this drill",
                                    value=st.session_state.blur_enabled.get(blur_key, False),
                                    key=f"enable_blur_{drill_type}"
                                )
                                st.session_state.blur_enabled[blur_key] = enable_blur
                                
                                if enable_blur:
                                    st.info("✏️ Draw on the image below to mark areas you want to blur in all clips")
                                    
                                    # Get first frame for blur mask creation
                                    with st.spinner("Extracting first frame..."):
                                        first_frame = get_video_first_frame(raw_video_info['bytes'])
                                    
                                    if first_frame is not None:
                                        # Convert to PIL Image
                                        pil_image = Image.fromarray(first_frame)
                                        
                                        # Resize for canvas (make it reasonable size)
                                        canvas_width = 640
                                        aspect_ratio = pil_image.height / pil_image.width
                                        canvas_height = int(canvas_width * aspect_ratio)
                                        
                                        # Resize the PIL image to canvas dimensions
                                        pil_image_resized = pil_image.resize((canvas_width, canvas_height), Image.Resampling.LANCZOS)
                                        
                                        col1, col2 = st.columns([3, 1])
                                        
                                        with col1:
                                            # Blur level selector
                                            if blur_key not in st.session_state.blur_levels:
                                                st.session_state.blur_levels[blur_key] = 2
                                            
                                            blur_level = st.select_slider(
                                                "Blur Intensity",
                                                options=[1, 2, 3],
                                                value=st.session_state.blur_levels.get(blur_key, 2),
                                                format_func=lambda x: {1: "Low", 2: "Medium", 3: "High"}[x],
                                                key=f"blur_level_{drill_type}"
                                            )
                                            st.session_state.blur_levels[blur_key] = blur_level
                                            
                                            # Drawing canvas
                                            canvas_result = st_canvas(
                                                fill_color="rgba(255, 0, 0, 0.3)",
                                                stroke_width=20,
                                                stroke_color="rgba(255, 0, 0, 0.8)",
                                                background_image=pil_image_resized,
                                                update_streamlit=True,
                                                height=canvas_height,
                                                width=canvas_width,
                                                drawing_mode="freedraw",
                                                key=f"canvas_{drill_type}",
                                            )
                                        
                                        with col2:
                                            st.write("**Controls:**")
                                            st.write("🖌️ Draw to mark blur area")
                                            st.write(f"📊 Blur Level: **{['Low', 'Medium', 'High'][blur_level-1]}**")
                                            
                                            # Undo button (clear canvas)
                                            if st.button("↩️ Clear Canvas", key=f"undo_blur_{drill_type}", use_container_width=True):
                                                st.session_state.blur_masks.pop(blur_key, None)
                                                st.session_state.blur_history.pop(blur_key, None)
                                                st.rerun()
                                            
                                            # Save blur mask button
                                            save_blur_button = st.button(
                                                "💾 Save Blur Area",
                                                key=f"save_blur_{drill_type}",
                                                use_container_width=True,
                                                type="primary"
                                            )
                                        
                                        if save_blur_button and canvas_result.image_data is not None:
                                            with st.spinner("Processing blur mask..."):
                                                try:
                                                    # Extract alpha channel as mask
                                                    canvas_image = canvas_result.image_data
                                                    
                                                    # Check if there are any drawings
                                                    if canvas_image[:, :, 3].max() > 0:
                                                        # Get alpha channel (drawings are non-zero)
                                                        mask = canvas_image[:, :, 3]
                                                        
                                                        # Resize mask to match original video dimensions
                                                        original_height, original_width = first_frame.shape[:2]
                                                        mask_resized = cv2.resize(mask, (original_width, original_height), interpolation=cv2.INTER_NEAREST)
                                                        
                                                        # Threshold to binary mask
                                                        _, mask_binary = cv2.threshold(mask_resized, 10, 255, cv2.THRESH_BINARY)
                                                        
                                                        # Store mask in session state
                                                        st.session_state.blur_masks[blur_key] = mask_binary
                                                        st.session_state.blur_history[blur_key] = {
                                                            'mask': mask_binary,
                                                            'level': blur_level
                                                        }
                                                        
                                                        st.success("✅ Blur area saved! This will be applied to all clips.")
                                                        
                                                        # Show preview of blurred first frame (resized for preview)
                                                        preview_frame = apply_blur_to_frame(first_frame, mask_binary, blur_level)
                                                        preview_height, preview_width = preview_frame.shape[:2]
                                                        preview_frame_resized = cv2.resize(
                                                            preview_frame, 
                                                            (preview_width // 4, preview_height // 4), 
                                                            interpolation=cv2.INTER_AREA
                                                        )
                                                        st.write("**Preview:**")
                                                        st.image(preview_frame_resized, caption="Blur Preview (Reduced Size)")
                                                    else:
                                                        st.warning("⚠️ No blur area drawn. Please draw on the canvas first.")
                                                
                                                except Exception as e:
                                                    st.error(f"Error saving blur mask: {e}")
                                        
                                        # Show saved blur info
                                        if blur_key in st.session_state.blur_masks:
                                            st.success(f"✅ Blur area saved (Level: {['Low', 'Medium', 'High'][st.session_state.blur_levels[blur_key]-1]})")
                                    else:
                                        st.error("❌ Could not extract first frame from video")
                                        st.warning("""
                                        **Troubleshooting:**
                                        - Ensure OpenCV is properly installed: `opencv-python-headless`
                                        - Check the server logs for detailed error messages
                                        - Verify the video file is not corrupted
                                        - Try reloading the page and searching again
                                        
                                        **For deployment issues:** Make sure `packages.txt` includes required system libraries.
                                        """)
                                else:
                                    # Clear blur if disabled
                                    if blur_key in st.session_state.blur_masks:
                                        st.session_state.blur_masks.pop(blur_key, None)
                                        st.session_state.blur_history.pop(blur_key, None)
                                
                                # Clip time inputs
                                st.divider()
                                st.subheader("⏱️ Define Clip Time Ranges")
                                
                                # Initialize clip length state
                                if f"{drill_type}_clip_length" not in st.session_state:
                                    st.session_state[f"{drill_type}_clip_length"] = 3
                                
                                # Clip length input (applies to all clips)
                                clip_length = st.number_input(
                                    "Clip Length (seconds) - applies to all clips",
                                    min_value=1,
                                    max_value=int(video_duration),
                                    value=st.session_state[f"{drill_type}_clip_length"],
                                    step=1,
                                    key=f"clip_length_{drill_type}"
                                )
                                st.session_state[f"{drill_type}_clip_length"] = clip_length
                                st.info(f"📏 All clips will be {clip_length} seconds long")
                                
                                if drill_type not in st.session_state.manual_clip_times:
                                    st.session_state.manual_clip_times[drill_type] = []
                                
                                # Ensure we have the right number of entries
                                while len(st.session_state.manual_clip_times[drill_type]) < num_clips:
                                    st.session_state.manual_clip_times[drill_type].append({'start': 0, 'end': clip_length})
                                while len(st.session_state.manual_clip_times[drill_type]) > num_clips:
                                    st.session_state.manual_clip_times[drill_type].pop()
                                
                                # Display start time inputs in 6-column grid
                                all_valid = True
                                for row in range(0, num_clips, 6):
                                    cols = st.columns(6)
                                    
                                    for col_idx in range(6):
                                        i = row + col_idx
                                        if i >= num_clips:
                                            break
                                        
                                        with cols[col_idx]:
                                            start_time = st.number_input(
                                                f"Clip {i+1} Start (s)",
                                                min_value=0,
                                                max_value=int(video_duration) - clip_length,
                                                value=int(st.session_state.manual_clip_times[drill_type][i]['start']),
                                                step=1,
                                                key=f"start_{drill_type}_{i}"
                                            )
                                            
                                            # Calculate end time automatically
                                            end_time = start_time + clip_length
                                            
                                            # Store both start and end time
                                            st.session_state.manual_clip_times[drill_type][i]['start'] = float(start_time)
                                            st.session_state.manual_clip_times[drill_type][i]['end'] = float(end_time)
                                            
                                            # Validation
                                            if end_time > video_duration:
                                                st.error(f"❌ Exceeds duration", icon="⚠️")
                                                all_valid = False
                                            else:
                                                st.caption(f"→ {end_time}s")
                                
                                st.write("")
                                
                                # Save New Clips button
                                st.divider()
                                col1, col2 = st.columns([1, 3])
                                with col1:
                                    save_clips_button = st.button(
                                        "💾 Save New Clips",
                                        key=f"save_clips_{drill_type}",
                                        use_container_width=True,
                                        disabled=not all_valid or st.session_state.manual_clips_created.get(drill_type, False)
                                    )
                                
                                if save_clips_button and all_valid:
                                    with st.spinner(f"Creating {num_clips} clips..."):
                                        try:
                                            player_name = st.session_state.player_name.lower().replace(' ', '_')
                                            created_clips = []
                                            
                                            # Get blur mask and level if enabled
                                            blur_key = f"{st.session_state.assessment_id}_{drill_type}"
                                            blur_mask = st.session_state.blur_masks.get(blur_key, None)
                                            blur_level = st.session_state.blur_levels.get(blur_key, 1)
                                            
                                            if blur_mask is not None:
                                                st.info(f"🎨 Applying blur (Level: {['Low', 'Medium', 'High'][blur_level-1]}) to all clips...")
                                            
                                            progress_bar = st.progress(0)
                                            
                                            error_messages = []
                                            for i in range(num_clips):
                                                start_time = st.session_state.manual_clip_times[drill_type][i]['start']
                                                end_time = st.session_state.manual_clip_times[drill_type][i]['end']
                                                
                                                clip_name = f"{player_name}_{drill_type}_clip_{i+1}.mp4"
                                                
                                                clip_bytes, error_msg = create_clip_from_video(
                                                    raw_video_info['bytes'],
                                                    start_time,
                                                    end_time,
                                                    clip_name,
                                                    blur_mask=blur_mask,
                                                    blur_level=blur_level
                                                )
                                                
                                                if clip_bytes:
                                                    created_clips.append({
                                                        'name': clip_name,
                                                        'bytes': clip_bytes,
                                                        'start': start_time,
                                                        'end': end_time,
                                                        'duration': end_time - start_time
                                                    })
                                                else:
                                                    error_messages.append(f"Clip {i+1}: {error_msg}")
                                                    st.error(f"❌ Failed to create clip {i+1}: {error_msg}")
                                                
                                                progress_bar.progress((i + 1) / num_clips)
                                            
                                            progress_bar.empty()
                                            
                                            if len(created_clips) == num_clips:
                                                st.session_state.processed_clips[drill_type] = created_clips
                                                st.session_state.manual_clips_created[drill_type] = True
                                                # Cache manual clips
                                                manual_cache_key = f"{st.session_state.assessment_id}_{drill_type}_manual"
                                                st.session_state.manual_clips_cache[manual_cache_key] = created_clips
                                                st.success(f"✅ Successfully created {num_clips} clips!")
                                                st.rerun()
                                            else:
                                                st.error(f"❌ Some clips failed to create ({len(created_clips)}/{num_clips} successful)")
                                                if error_messages:
                                                    with st.expander("View Error Details"):
                                                        for err in error_messages:
                                                            st.write(f"• {err}")
                                        
                                        except Exception as e:
                                            st.error(f"Error creating clips: {e}")
                                
                                # Display created clips
                                if st.session_state.manual_clips_created.get(drill_type, False) and drill_type in st.session_state.processed_clips:
                                    st.divider()
                                    st.subheader("📹 Created Clips")
                                    
                                    # Display clips in grid
                                    for row in range(0, len(st.session_state.processed_clips[drill_type]), 3):
                                        cols = st.columns(3)
                                        
                                        for col_idx, clip_data in enumerate(st.session_state.processed_clips[drill_type][row:row+3]):
                                            clip_idx = row + col_idx
                                            with cols[col_idx]:
                                                st.write(f"**Clip {clip_idx + 1}**")
                                                # Only show timing info if it exists (manual clips)
                                                if 'start' in clip_data and 'end' in clip_data and 'duration' in clip_data:
                                                    st.write(f"Start: {clip_data['start']:.2f}s | End: {clip_data['end']:.2f}s")
                                                    st.write(f"Duration: {clip_data['duration']:.2f}s")
                                                st.video(clip_data['bytes'])
                                    
                                    # Continue to Upload & Flyte Task flow
                                    st.divider()
                                    st.info("✅ Clips created! Continue below to upload and run Flyte task.")
                        else:
                            st.error("❌ drill_id or activity_id not found for this drill. Cannot load raw video.")
                            st.write("This drill may not support manual clipping.")
                    
                    # If we're in auto clip mode, load auto-generated clips
                    if st.session_state.clip_selection_mode.get(drill_type) == 'auto':
                        st.info("📹 Auto-Generated Clips Mode: Select from pre-processed clips")
                        st.divider()
                
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
                    if drill_type not in st.session_state.processed_clips:
                        with st.spinner('Downloading and processing running drill video...'):
                            try:
                                bucket_name, video_object_name = parse_gsutil_url(drill_info['video_url'])
                                player_name = st.session_state.player_name.lower().replace(' ', '_')
                                video_name = f"{player_name}_{drill_type}_video.{video_object_name.split('.')[-1]}"
                                
                                video_bytes = download_gcs_file_to_bytes(bucket_name, video_object_name)
                                
                                if video_bytes:
                                    processed_bytes = process_clip_to_bytes(video_bytes, video_name)
                                    
                                    if processed_bytes:
                                        st.session_state.processed_clips[drill_type] = [{
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
                    if drill_type in st.session_state.processed_clips:
                        st.write("**Video Preview:**")
                        video_data = st.session_state.processed_clips[drill_type][0]
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
                    # Only download clips if in auto mode
                    if st.session_state.clip_selection_mode.get(drill_type) == 'auto':
                        # Check cache first
                        auto_cache_key = f"{st.session_state.assessment_id}_{drill_type}_auto"
                        
                        if auto_cache_key in st.session_state.auto_clips_cache:
                            if drill_type not in st.session_state.processed_clips:
                                st.session_state.processed_clips[drill_type] = st.session_state.auto_clips_cache[auto_cache_key]
                                st.info("📦 Loaded auto-generated clips from cache!")
                        elif drill_type not in st.session_state.processed_clips:
                            # Download and process clips
                            processed_clips, error = download_and_process_clips(
                                st.session_state.assessment_id,
                                st.session_state.player_name,
                                drill_type,
                                drill_info['clips']
                            )
                            
                            if error:
                                st.error(error)
                            else:
                                st.session_state.processed_clips[drill_type] = processed_clips
                                # Cache auto clips
                                st.session_state.auto_clips_cache[auto_cache_key] = processed_clips
                                st.success(f"Processed {len(processed_clips)} clips!")
                    
                    if drill_type in st.session_state.processed_clips:
                        # Check if we're in manual clipping mode with created clips
                        in_manual_mode = st.session_state.clip_selection_mode.get(drill_type) == 'manual' and st.session_state.manual_clips_created.get(drill_type, False)
                        
                        if not in_manual_mode:
                            st.write(f"**Showing {len(st.session_state.processed_clips[drill_type])} clips**")
                        
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
                        
                        # If in manual mode, skip clip selection and auto-select all clips
                        if in_manual_mode:
                            st.info("🎬 Manual clips created! All clips are automatically selected for upload.")
                            # Auto-select all manual clips
                            st.session_state.selected_clips[drill_type] = list(range(len(st.session_state.processed_clips[drill_type])))
                        else:
                            # Normal clip selection flow
                            if drill_type not in st.session_state.selected_clips:
                                st.session_state.selected_clips[drill_type] = []
                            
                            clips_to_show = st.session_state.processed_clips[drill_type][:12]
                            
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
                            
                            if len(st.session_state.processed_clips[drill_type]) > 12:
                                st.info(f"Showing first 12 of {len(st.session_state.processed_clips[drill_type])} clips")
                        
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
                                    st.session_state.processed_clips[drill_type]
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
                                    
                                    defense_clips_to_show = defense_clips[:12]
                                    
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
                                    
                                    if len(defense_clips) > 12:
                                        st.info(f"Showing first 12 of {len(defense_clips)} defense clips")
                                    
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
                                                            st.error(f"❌ Failed to update metrics — no document found with assessment_id=`{st.session_state.assessment_id}` in MongoDB. Check the console logs for details.")
                                                    
                                                    except Exception as e:
                                                        st.error(f"Error updating metrics: {str(e)}")
                                                        st.exception(e)
                    else:
                        st.info("👆 Select clips above to run Flyte task")
        else:
            st.warning("No drills found")

# ============================================================================
# TAB 2 — MANUAL GENERATION (new flow)
# ============================================================================
with tab_manual:
    st.header("Manual Generation")
    st.write("Upload videos directly and generate drill metrics without a pre-existing assessment record.")

    st.divider()

    # Assessment ID + Player Name inputs
    mg_col1, mg_col2, mg_col3 = st.columns([2, 2, 1])
    with mg_col1:
        mg_assessment_id_input = st.text_input(
            "Assessment ID",
            value=st.session_state.mg_assessment_id_val,
            placeholder="Enter or paste an assessment ID",
            key="mg_assessment_id_input"
        )
    with mg_col2:
        mg_player_name_input = st.text_input(
            "Player Name",
            value=st.session_state.mg_player_name_val,
            placeholder="Enter player full name",
            key="mg_player_name_input"
        )
    with mg_col3:
        st.write("")
        st.write("")
        mg_start_btn = st.button("▶️ Start", type="primary", use_container_width=True, key="mg_start_btn")

    if mg_start_btn:
        if not mg_assessment_id_input or not mg_player_name_input:
            st.error("Please enter both Assessment ID and Player Name.")
        else:
            # Save and reset all mg_ state for the new session
            st.session_state.mg_assessment_id_val = mg_assessment_id_input
            st.session_state.mg_player_name_val = mg_player_name_input
            st.session_state.mg_assessment_started = True
            st.session_state.mg_raw_video_data = {}
            st.session_state.mg_num_manual_clips = {}
            st.session_state.mg_manual_clip_times = {}
            st.session_state.mg_manual_clips_created = {}
            st.session_state.mg_processed_clips = {}
            st.session_state.mg_blur_masks = {}
            st.session_state.mg_blur_levels = {}
            st.session_state.mg_blur_history = {}
            st.session_state.mg_blur_enabled = {}
            st.session_state.mg_signed_urls = {}
            st.session_state.mg_execution_ids = {}
            st.session_state.mg_task_status = {}
            st.session_state.mg_saved_coach_feedback = {}
            st.session_state.mg_running_drill_inputs = {}
            st.session_state.mg_drill_metrics_data = {}
            st.session_state.mg_edited_angles = {}
            st.session_state.mg_edited_directions = {}
            st.session_state.mg_original_values = {}
            st.session_state.mg_mongo_drill_keys = {}
            st.rerun()

    if st.session_state.mg_assessment_started and st.session_state.mg_assessment_id_val and st.session_state.mg_player_name_val:
        mg_assessment_id = st.session_state.mg_assessment_id_val
        mg_player_name = st.session_state.mg_player_name_val

        st.divider()
        st.subheader(f"Player: {mg_player_name}  |  Assessment: `{mg_assessment_id}`")

        # 8 drill type tabs
        MG_DRILL_TAB_NAMES = [
            "Top Hand", "Bottom Hand", "Backfoot Drive", "Backfoot Defense",
            "Power Hitting", "Hold Your Pose", "Running Drill", "Feet Planted"
        ]
        MG_DRILL_TYPE_KEYS = [
            'tophand', 'bottomhand', 'backfoot_drive', 'backfoot_defense',
            'power_hitting', 'hold_your_pose', 'running_drill', 'feet_planted'
        ]

        mg_drill_tabs = st.tabs(MG_DRILL_TAB_NAMES)

        for mg_tab, mg_drill_type in zip(mg_drill_tabs, MG_DRILL_TYPE_KEYS):
            with mg_tab:

                # ----------------------------------------------------------------
                # RUNNING DRILL — single video, no manual clipping
                # ----------------------------------------------------------------
                if mg_drill_type == 'running_drill':
                    st.subheader("🏃 Running Drill")

                    mg_rd_col1, mg_rd_col2 = st.columns(2)
                    with mg_rd_col1:
                        mg_gender = st.selectbox(
                            "Gender",
                            options=["male", "female"],
                            key=f"mg_gender_{mg_drill_type}"
                        )
                    with mg_rd_col2:
                        if mg_gender == "male":
                            mg_cat_opts = ["open", "u23", "u19", "u16", "u14"]
                        else:
                            mg_cat_opts = ["open", "u23", "u19", "u15"]
                        mg_category = st.selectbox(
                            "Category",
                            options=mg_cat_opts,
                            key=f"mg_category_{mg_drill_type}"
                        )

                    st.session_state.mg_running_drill_inputs['gender'] = mg_gender
                    st.session_state.mg_running_drill_inputs['category'] = mg_category

                    mg_rd_file = st.file_uploader(
                        "Upload Running Drill Video",
                        type=['mp4', 'mov', 'avi', 'mkv'],
                        key=f"mg_file_{mg_drill_type}"
                    )

                    mg_rd_raw_key = f"mg_{mg_drill_type}"
                    if mg_rd_file is not None:
                        cached = st.session_state.mg_raw_video_data.get(mg_rd_raw_key, {})
                        if cached.get('filename') != mg_rd_file.name:
                            with st.spinner("Processing uploaded video..."):
                                rd_video_bytes = mg_rd_file.read()
                                rd_duration = get_video_duration(rd_video_bytes)
                                rd_processed = process_clip_to_bytes(rd_video_bytes, f"{mg_drill_type}.mp4")
                                st.session_state.mg_raw_video_data[mg_rd_raw_key] = {
                                    'bytes': rd_video_bytes,
                                    'processed_bytes': rd_processed,
                                    'duration': rd_duration,
                                    'filename': mg_rd_file.name
                                }
                                st.session_state.mg_processed_clips[mg_drill_type] = [{
                                    'name': f"{mg_player_name.lower().replace(' ', '_').replace('.', '_')}_{mg_drill_type}.mp4",
                                    'bytes': rd_processed or rd_video_bytes,
                                }]
                                st.session_state.mg_signed_urls.pop(mg_drill_type, None)
                                st.session_state.mg_execution_ids.pop(mg_drill_type, None)

                    if mg_rd_raw_key in st.session_state.mg_raw_video_data:
                        mg_rd_info = st.session_state.mg_raw_video_data[mg_rd_raw_key]
                        st.video(mg_rd_info.get('processed_bytes') or mg_rd_info['bytes'])
                        st.info(f"📹 Duration: {mg_rd_info['duration']:.2f}s")

                        # Coach feedback
                        st.divider()
                        st.subheader("📝 Coach Feedback (Optional)")
                        mg_rd_fb_key = f"{mg_assessment_id}_{mg_drill_type}"
                        mg_rd_fb_col1, mg_rd_fb_col2 = st.columns([4, 1])
                        with mg_rd_fb_col1:
                            mg_rd_coach_feedback = st.text_area(
                                "Coach Feedback",
                                value=st.session_state.mg_saved_coach_feedback.get(mg_rd_fb_key, ''),
                                key=f"mg_feedback_{mg_drill_type}",
                                placeholder="Enter coach feedback...",
                                height=100
                            )
                        with mg_rd_fb_col2:
                            st.write("")
                            st.write("")
                            if st.button("💾 Save", key=f"mg_save_fb_{mg_drill_type}", use_container_width=True):
                                with st.spinner("Saving..."):
                                    if save_coach_feedback_to_mongo(mg_assessment_id, mg_drill_type, mg_rd_coach_feedback):
                                        st.session_state.mg_saved_coach_feedback[mg_rd_fb_key] = mg_rd_coach_feedback
                                        st.success("✅ Saved!")
                                    else:
                                        st.error("❌ Failed to save")

                        # GCS Upload + Flyte
                        st.divider()
                        st.subheader("🚀 Run Flyte Task")

                        mg_rd_up_col1, _ = st.columns([1, 3])
                        with mg_rd_up_col1:
                            mg_rd_upload_btn = st.button(
                                "☁️ Upload to GCS",
                                key=f"mg_upload_{mg_drill_type}",
                                use_container_width=True,
                                disabled=mg_drill_type in st.session_state.mg_signed_urls
                            )

                        if mg_rd_upload_btn:
                            with st.spinner("Uploading to GCS..."):
                                try:
                                    mg_pn_fmt = mg_player_name.lower().replace(' ', '_').replace('.', '_')
                                    mg_folder = f"{GCS_PARENT_FOLDER_NAME}{mg_pn_fmt}_{mg_assessment_id}/"
                                    create_gcs_folder(GCS_BUCKET_NAME, mg_folder)
                                    mg_rd_clip = st.session_state.mg_processed_clips[mg_drill_type][0]
                                    mg_video_to_up = mg_rd_info.get('processed_bytes') or mg_rd_info['bytes']
                                    with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as mg_tmp:
                                        mg_tmp.write(mg_video_to_up)
                                        mg_tmp_path = mg_tmp.name
                                    try:
                                        mg_signed_url, _ = upload_to_gcs(
                                            GCS_BUCKET_NAME,
                                            mg_tmp_path,
                                            mg_folder + mg_rd_clip['name'],
                                            signed_url_flag=True
                                        )
                                        st.session_state.mg_signed_urls[mg_drill_type] = [mg_signed_url]
                                        st.success("✅ Uploaded to GCS!")
                                    finally:
                                        if os.path.exists(mg_tmp_path):
                                            os.unlink(mg_tmp_path)
                                except Exception as e:
                                    st.error(f"Upload error: {e}")

                        if mg_drill_type in st.session_state.mg_signed_urls:
                            st.success("✅ Video uploaded to GCS")

                            # Use mg_running_drill_inputs for gender/category
                            mg_rd_flyte_inputs = build_flyte_inputs(
                                mg_drill_type,
                                mg_player_name,
                                mg_assessment_id,
                                st.session_state.mg_signed_urls[mg_drill_type][0],
                                mg_rd_coach_feedback
                            )
                            # Override gender/category from mg inputs
                            mg_rd_flyte_inputs['inputs']['gender'] = st.session_state.mg_running_drill_inputs.get('gender', 'male')
                            mg_rd_flyte_inputs['inputs']['category'] = st.session_state.mg_running_drill_inputs.get('category', 'u19')

                            st.write("**Flyte Task Inputs:**")
                            st.json(mg_rd_flyte_inputs, expanded=False)

                            mg_rd_run_col1, _ = st.columns([1, 3])
                            with mg_rd_run_col1:
                                mg_rd_run_btn = st.button(
                                    "▶️ Run Task",
                                    key=f"mg_run_{mg_drill_type}",
                                    use_container_width=True,
                                    disabled=mg_drill_type in st.session_state.mg_execution_ids
                                )

                            if mg_rd_run_btn:
                                with st.spinner("Submitting task..."):
                                    try:
                                        mg_rd_resp = run_task(mg_rd_flyte_inputs)
                                        mg_rd_exec_id = mg_rd_resp.get("execution_id")
                                        st.session_state.mg_execution_ids[mg_drill_type] = mg_rd_exec_id
                                        st.session_state.mg_task_status[mg_drill_type] = {
                                            "execution_id": mg_rd_exec_id,
                                            "status": "submitted",
                                            "is_done": False
                                        }
                                        st.success(f"✅ Task submitted! ID: {mg_rd_exec_id}")
                                    except Exception as e:
                                        st.error(f"Error: {e}")

                            if mg_drill_type in st.session_state.mg_execution_ids:
                                st.divider()
                                st.subheader("📊 Task Status")
                                mg_rd_exec_id = st.session_state.mg_execution_ids[mg_drill_type]
                                st.write(f"**Execution ID:** `{mg_rd_exec_id}`")

                                mg_rd_st_col1, _ = st.columns([1, 3])
                                with mg_rd_st_col1:
                                    mg_rd_check_btn = st.button(
                                        "🔄 Check Status",
                                        key=f"mg_status_{mg_drill_type}",
                                        use_container_width=True
                                    )

                                if mg_rd_check_btn:
                                    with st.spinner("Fetching status..."):
                                        try:
                                            mg_rd_status = fetch_task_execution_post({"execution_id": mg_rd_exec_id})
                                            st.session_state.mg_task_status[mg_drill_type] = mg_rd_status
                                        except Exception as e:
                                            st.error(f"Error: {e}")

                                if mg_drill_type in st.session_state.mg_task_status:
                                    mg_rd_status_data = st.session_state.mg_task_status[mg_drill_type]
                                    mg_rd_is_done = mg_rd_status_data.get("is_done", False)
                                    if mg_rd_is_done:
                                        st.success("✅ Task Completed!")
                                    else:
                                        st.info("⏳ Task In Progress...")
                                    with st.expander("View Full Status", expanded=mg_rd_is_done):
                                        st.json(mg_rd_status_data)

                # ----------------------------------------------------------------
                # ALL OTHER DRILLS — manual clipping flow
                # ----------------------------------------------------------------
                else:
                    drill_label = mg_drill_type.replace('_', ' ').title()
                    st.subheader(f"📹 {drill_label}")

                    mg_file = st.file_uploader(
                        f"Upload Video for {drill_label}",
                        type=['mp4', 'mov', 'avi', 'mkv'],
                        key=f"mg_file_{mg_drill_type}"
                    )

                    mg_raw_key = f"mg_{mg_drill_type}"
                    if mg_file is not None:
                        mg_cached = st.session_state.mg_raw_video_data.get(mg_raw_key, {})
                        if mg_cached.get('filename') != mg_file.name:
                            with st.spinner("Loading video..."):
                                mg_video_bytes = mg_file.read()
                                mg_duration = get_video_duration(mg_video_bytes)
                                st.session_state.mg_raw_video_data[mg_raw_key] = {
                                    'bytes': mg_video_bytes,
                                    'duration': mg_duration,
                                    'filename': mg_file.name
                                }
                                # Reset clip state when new file is uploaded
                                st.session_state.mg_manual_clips_created[mg_drill_type] = False
                                st.session_state.mg_processed_clips.pop(mg_drill_type, None)
                                st.session_state.mg_signed_urls.pop(mg_drill_type, None)
                                st.session_state.mg_execution_ids.pop(mg_drill_type, None)
                                st.session_state.mg_manual_clip_times.pop(mg_drill_type, None)
                                st.success(f"✅ Video loaded! Duration: {mg_duration:.2f}s")

                    if mg_raw_key in st.session_state.mg_raw_video_data:
                        mg_raw_info = st.session_state.mg_raw_video_data[mg_raw_key]
                        mg_video_duration = mg_raw_info['duration']

                        mg_vid_col, mg_ctrl_col = st.columns([1, 3])
                        with mg_vid_col:
                            st.write("**Video Preview:**")
                            st.video(mg_raw_info['bytes'])
                            st.info(f"📹 Duration: {mg_video_duration:.2f}s")

                        with mg_ctrl_col:
                            st.write("**Clip Settings:**")
                            if mg_drill_type not in st.session_state.mg_num_manual_clips:
                                st.session_state.mg_num_manual_clips[mg_drill_type] = 1

                            mg_num_clips = st.number_input(
                                "Number of clips (1-6)",
                                min_value=1,
                                max_value=6,
                                value=st.session_state.mg_num_manual_clips[mg_drill_type],
                                key=f"mg_num_clips_{mg_drill_type}"
                            )
                            st.session_state.mg_num_manual_clips[mg_drill_type] = mg_num_clips

                        # Blur Area Selection
                        st.divider()
                        st.subheader("🎨 Blur Area Selection (Optional)")
                        mg_blur_key = f"mg_{mg_drill_type}"

                        if mg_blur_key not in st.session_state.mg_blur_enabled:
                            st.session_state.mg_blur_enabled[mg_blur_key] = False

                        mg_enable_blur = st.checkbox(
                            "Enable blur for this drill",
                            value=st.session_state.mg_blur_enabled.get(mg_blur_key, False),
                            key=f"mg_enable_blur_{mg_drill_type}"
                        )
                        st.session_state.mg_blur_enabled[mg_blur_key] = mg_enable_blur

                        if mg_enable_blur:
                            st.info("✏️ Draw on the image below to mark areas to blur in all clips")

                            with st.spinner("Extracting first frame..."):
                                mg_first_frame = get_video_first_frame(mg_raw_info['bytes'])

                            if mg_first_frame is not None:
                                mg_pil_img = Image.fromarray(mg_first_frame)
                                mg_canvas_w = 640
                                mg_canvas_h = int(mg_canvas_w * mg_pil_img.height / mg_pil_img.width)
                                mg_pil_resized = mg_pil_img.resize((mg_canvas_w, mg_canvas_h), Image.Resampling.LANCZOS)

                                mg_blur_draw_col, mg_blur_ctrl_col = st.columns([3, 1])

                                with mg_blur_draw_col:
                                    if mg_blur_key not in st.session_state.mg_blur_levels:
                                        st.session_state.mg_blur_levels[mg_blur_key] = 2

                                    mg_blur_level = st.select_slider(
                                        "Blur Intensity",
                                        options=[1, 2, 3],
                                        value=st.session_state.mg_blur_levels.get(mg_blur_key, 2),
                                        format_func=lambda x: {1: "Low", 2: "Medium", 3: "High"}[x],
                                        key=f"mg_blur_level_{mg_drill_type}"
                                    )
                                    st.session_state.mg_blur_levels[mg_blur_key] = mg_blur_level

                                    mg_canvas_result = st_canvas(
                                        fill_color="rgba(255, 0, 0, 0.3)",
                                        stroke_width=20,
                                        stroke_color="rgba(255, 0, 0, 0.8)",
                                        background_image=mg_pil_resized,
                                        update_streamlit=True,
                                        height=mg_canvas_h,
                                        width=mg_canvas_w,
                                        drawing_mode="freedraw",
                                        key=f"mg_canvas_{mg_drill_type}",
                                    )

                                with mg_blur_ctrl_col:
                                    st.write("**Controls:**")
                                    st.write("🖌️ Draw to mark blur area")
                                    st.write(f"📊 Level: **{['Low', 'Medium', 'High'][mg_blur_level-1]}**")

                                    if st.button("↩️ Clear Canvas", key=f"mg_undo_blur_{mg_drill_type}", use_container_width=True):
                                        st.session_state.mg_blur_masks.pop(mg_blur_key, None)
                                        st.session_state.mg_blur_history.pop(mg_blur_key, None)
                                        st.rerun()

                                    mg_save_blur_btn = st.button(
                                        "💾 Save Blur Area",
                                        key=f"mg_save_blur_{mg_drill_type}",
                                        use_container_width=True,
                                        type="primary"
                                    )

                                if mg_save_blur_btn and mg_canvas_result.image_data is not None:
                                    with st.spinner("Processing blur mask..."):
                                        try:
                                            mg_canvas_img = mg_canvas_result.image_data
                                            if mg_canvas_img[:, :, 3].max() > 0:
                                                mg_mask_alpha = mg_canvas_img[:, :, 3]
                                                mg_orig_h, mg_orig_w = mg_first_frame.shape[:2]
                                                mg_mask_resized = cv2.resize(mg_mask_alpha, (mg_orig_w, mg_orig_h), interpolation=cv2.INTER_NEAREST)
                                                _, mg_mask_binary = cv2.threshold(mg_mask_resized, 10, 255, cv2.THRESH_BINARY)
                                                st.session_state.mg_blur_masks[mg_blur_key] = mg_mask_binary
                                                st.session_state.mg_blur_history[mg_blur_key] = {
                                                    'mask': mg_mask_binary,
                                                    'level': mg_blur_level
                                                }
                                                st.success("✅ Blur area saved!")

                                                mg_preview = apply_blur_to_frame(mg_first_frame, mg_mask_binary, mg_blur_level)
                                                mg_ph, mg_pw = mg_preview.shape[:2]
                                                mg_preview_small = cv2.resize(mg_preview, (mg_pw // 4, mg_ph // 4), interpolation=cv2.INTER_AREA)
                                                st.image(mg_preview_small, caption="Blur Preview")
                                            else:
                                                st.warning("⚠️ No blur area drawn. Please draw on the canvas first.")
                                        except Exception as e:
                                            st.error(f"Error saving blur mask: {e}")

                                if mg_blur_key in st.session_state.mg_blur_masks:
                                    st.success(f"✅ Blur saved (Level: {['Low', 'Medium', 'High'][st.session_state.mg_blur_levels[mg_blur_key]-1]})")
                            else:
                                st.error("❌ Could not extract first frame from video")
                        else:
                            if mg_blur_key in st.session_state.mg_blur_masks:
                                st.session_state.mg_blur_masks.pop(mg_blur_key, None)

                        # Clip time inputs
                        st.divider()
                        st.subheader("⏱️ Define Clip Time Ranges")

                        mg_cl_key = f"mg_{mg_drill_type}_clip_length"
                        if mg_cl_key not in st.session_state:
                            st.session_state[mg_cl_key] = 3

                        mg_clip_length = st.number_input(
                            "Clip Length (seconds) — applies to all clips",
                            min_value=1,
                            max_value=max(1, int(mg_video_duration)),
                            value=st.session_state[mg_cl_key],
                            step=1,
                            key=f"mg_clip_length_{mg_drill_type}"
                        )
                        st.session_state[mg_cl_key] = mg_clip_length
                        st.info(f"📏 All clips will be {mg_clip_length} seconds long")

                        if mg_drill_type not in st.session_state.mg_manual_clip_times:
                            st.session_state.mg_manual_clip_times[mg_drill_type] = []

                        while len(st.session_state.mg_manual_clip_times[mg_drill_type]) < mg_num_clips:
                            st.session_state.mg_manual_clip_times[mg_drill_type].append({'start': 0, 'end': mg_clip_length})
                        while len(st.session_state.mg_manual_clip_times[mg_drill_type]) > mg_num_clips:
                            st.session_state.mg_manual_clip_times[mg_drill_type].pop()

                        mg_all_valid = True
                        for mg_row in range(0, mg_num_clips, 6):
                            mg_cols = st.columns(6)
                            for mg_col_idx in range(6):
                                mg_i = mg_row + mg_col_idx
                                if mg_i >= mg_num_clips:
                                    break
                                with mg_cols[mg_col_idx]:
                                    mg_max_start = max(0, int(mg_video_duration) - mg_clip_length)
                                    mg_start = st.number_input(
                                        f"Clip {mg_i+1} Start (s)",
                                        min_value=0,
                                        max_value=mg_max_start,
                                        value=int(st.session_state.mg_manual_clip_times[mg_drill_type][mg_i]['start']),
                                        step=1,
                                        key=f"mg_start_{mg_drill_type}_{mg_i}"
                                    )
                                    mg_end = mg_start + mg_clip_length
                                    st.session_state.mg_manual_clip_times[mg_drill_type][mg_i]['start'] = float(mg_start)
                                    st.session_state.mg_manual_clip_times[mg_drill_type][mg_i]['end'] = float(mg_end)

                                    if mg_end > mg_video_duration:
                                        st.error("❌ Exceeds duration")
                                        mg_all_valid = False
                                    else:
                                        st.caption(f"→ {mg_end}s")

                        # Create Clips button
                        st.divider()
                        mg_create_col1, _ = st.columns([1, 3])
                        with mg_create_col1:
                            mg_create_btn = st.button(
                                "💾 Create Clips",
                                key=f"mg_create_clips_{mg_drill_type}",
                                use_container_width=True,
                                disabled=not mg_all_valid or st.session_state.mg_manual_clips_created.get(mg_drill_type, False)
                            )

                        if mg_create_btn and mg_all_valid:
                            with st.spinner(f"Creating {mg_num_clips} clips..."):
                                try:
                                    mg_pn_fmt = mg_player_name.lower().replace(' ', '_').replace('.', '_')
                                    mg_created = []
                                    mg_blur_mask_val = st.session_state.mg_blur_masks.get(mg_blur_key, None)
                                    mg_blur_lv_val = st.session_state.mg_blur_levels.get(mg_blur_key, 1)

                                    if mg_blur_mask_val is not None:
                                        st.info(f"🎨 Applying blur (Level: {['Low', 'Medium', 'High'][mg_blur_lv_val-1]}) to all clips...")

                                    mg_prog = st.progress(0)
                                    mg_errs = []

                                    for mg_i in range(mg_num_clips):
                                        mg_st = st.session_state.mg_manual_clip_times[mg_drill_type][mg_i]['start']
                                        mg_en = st.session_state.mg_manual_clip_times[mg_drill_type][mg_i]['end']
                                        mg_clip_name = f"{mg_pn_fmt}_{mg_drill_type}_clip_{mg_i+1}.mp4"

                                        mg_cb, mg_err = create_clip_from_video(
                                            mg_raw_info['bytes'],
                                            mg_st, mg_en, mg_clip_name,
                                            blur_mask=mg_blur_mask_val,
                                            blur_level=mg_blur_lv_val
                                        )

                                        if mg_cb:
                                            mg_created.append({
                                                'name': mg_clip_name,
                                                'bytes': mg_cb,
                                                'start': mg_st,
                                                'end': mg_en,
                                                'duration': mg_en - mg_st
                                            })
                                        else:
                                            mg_errs.append(f"Clip {mg_i+1}: {mg_err}")
                                            st.error(f"❌ Failed clip {mg_i+1}: {mg_err}")

                                        mg_prog.progress((mg_i + 1) / mg_num_clips)

                                    mg_prog.empty()

                                    if len(mg_created) == mg_num_clips:
                                        st.session_state.mg_processed_clips[mg_drill_type] = mg_created
                                        st.session_state.mg_manual_clips_created[mg_drill_type] = True
                                        st.success(f"✅ Successfully created {mg_num_clips} clips!")
                                        st.rerun()
                                    else:
                                        st.error(f"❌ {len(mg_created)}/{mg_num_clips} clips created")
                                        if mg_errs:
                                            with st.expander("Error Details"):
                                                for mg_e in mg_errs:
                                                    st.write(f"• {mg_e}")
                                except Exception as e:
                                    st.error(f"Error creating clips: {e}")

                        # Display created clips + upload/Flyte flow
                        if st.session_state.mg_manual_clips_created.get(mg_drill_type, False) and mg_drill_type in st.session_state.mg_processed_clips:
                            st.divider()
                            st.subheader("📹 Created Clips")

                            for mg_r in range(0, len(st.session_state.mg_processed_clips[mg_drill_type]), 3):
                                mg_c_cols = st.columns(3)
                                for mg_ci, mg_cd in enumerate(st.session_state.mg_processed_clips[mg_drill_type][mg_r:mg_r+3]):
                                    with mg_c_cols[mg_ci]:
                                        st.write(f"**Clip {mg_r + mg_ci + 1}**")
                                        if 'start' in mg_cd:
                                            st.write(f"Start: {mg_cd['start']:.2f}s | End: {mg_cd['end']:.2f}s | Duration: {mg_cd['duration']:.2f}s")
                                        st.video(mg_cd['bytes'])

                            # Coach Feedback
                            st.divider()
                            st.subheader("📝 Coach Feedback (Optional)")
                            mg_fb_key = f"{mg_assessment_id}_{mg_drill_type}"
                            mg_fb_col1, mg_fb_col2 = st.columns([4, 1])
                            with mg_fb_col1:
                                mg_coach_fb = st.text_area(
                                    "Coach Feedback",
                                    value=st.session_state.mg_saved_coach_feedback.get(mg_fb_key, ''),
                                    key=f"mg_feedback_{mg_drill_type}",
                                    placeholder="Enter coach feedback...",
                                    height=100
                                )
                            with mg_fb_col2:
                                st.write("")
                                st.write("")
                                if st.button("💾 Save", key=f"mg_save_fb_{mg_drill_type}", use_container_width=True):
                                    with st.spinner("Saving..."):
                                        if save_coach_feedback_to_mongo(mg_assessment_id, mg_drill_type, mg_coach_fb):
                                            st.session_state.mg_saved_coach_feedback[mg_fb_key] = mg_coach_fb
                                            st.success("✅ Saved!")
                                        else:
                                            st.error("❌ Failed to save")

                            if st.session_state.mg_saved_coach_feedback.get(mg_fb_key, ''):
                                st.info(f"💾 Saved: {st.session_state.mg_saved_coach_feedback[mg_fb_key][:100]}...")

                            # GCS Upload
                            st.divider()
                            st.subheader("🚀 Run Flyte Task")
                            st.write(f"**{len(st.session_state.mg_processed_clips[mg_drill_type])} clips ready** — all will be uploaded.")

                            mg_up_col1, _ = st.columns([1, 3])
                            with mg_up_col1:
                                mg_upload_btn = st.button(
                                    "☁️ Upload to GCS",
                                    key=f"mg_upload_{mg_drill_type}",
                                    use_container_width=True,
                                    disabled=mg_drill_type in st.session_state.mg_signed_urls
                                )

                            if mg_upload_btn:
                                mg_all_idx = list(range(len(st.session_state.mg_processed_clips[mg_drill_type])))
                                mg_surls, mg_up_err = upload_selected_clips_to_gcs(
                                    mg_assessment_id,
                                    mg_player_name,
                                    mg_drill_type,
                                    mg_all_idx,
                                    st.session_state.mg_processed_clips[mg_drill_type]
                                )
                                if mg_up_err:
                                    st.error(mg_up_err)
                                else:
                                    st.session_state.mg_signed_urls[mg_drill_type] = mg_surls
                                    st.success(f"✅ Uploaded {len(mg_surls)} clips to GCS!")

                            if mg_drill_type in st.session_state.mg_signed_urls:
                                st.success(f"✅ {len(st.session_state.mg_signed_urls[mg_drill_type])} drive clips uploaded")

                                # ── Optional Backfoot Defense section (backfoot_drive only) ──
                                mg_def_urls = []
                                if mg_drill_type == 'backfoot_drive':
                                    st.divider()
                                    st.subheader("🛡️ Backfoot Defense Clips (Optional)")
                                    st.info("ℹ️ Optionally upload a backfoot defense video, create clips, and include them in the task.")

                                    mg_def_key = 'backfoot_drive_def'
                                    mg_def_raw_key = f"mg_{mg_def_key}"

                                    mg_def_file = st.file_uploader(
                                        "Upload Backfoot Defense Video",
                                        type=['mp4', 'mov', 'avi', 'mkv'],
                                        key="mg_file_backfoot_drive_def"
                                    )

                                    if mg_def_file is not None:
                                        mg_def_cached = st.session_state.mg_raw_video_data.get(mg_def_raw_key, {})
                                        if mg_def_cached.get('filename') != mg_def_file.name:
                                            with st.spinner("Loading defense video..."):
                                                mg_def_vb = mg_def_file.read()
                                                mg_def_dur_val = get_video_duration(mg_def_vb)
                                                st.session_state.mg_raw_video_data[mg_def_raw_key] = {
                                                    'bytes': mg_def_vb,
                                                    'duration': mg_def_dur_val,
                                                    'filename': mg_def_file.name
                                                }
                                                st.session_state.mg_manual_clips_created[mg_def_key] = False
                                                st.session_state.mg_processed_clips.pop(mg_def_key, None)
                                                st.session_state.mg_signed_urls.pop(mg_def_key, None)
                                                st.session_state.mg_manual_clip_times.pop(mg_def_key, None)
                                                st.success(f"✅ Defense video loaded! Duration: {mg_def_dur_val:.2f}s")

                                    if mg_def_raw_key in st.session_state.mg_raw_video_data:
                                        mg_def_info = st.session_state.mg_raw_video_data[mg_def_raw_key]
                                        mg_def_dur = mg_def_info['duration']

                                        mg_def_vid_col, mg_def_ctrl_col = st.columns([1, 3])
                                        with mg_def_vid_col:
                                            st.write("**Defense Video Preview:**")
                                            st.video(mg_def_info['bytes'])
                                            st.info(f"📹 Duration: {mg_def_dur:.2f}s")
                                        with mg_def_ctrl_col:
                                            st.write("**Defense Clip Settings:**")
                                            if mg_def_key not in st.session_state.mg_num_manual_clips:
                                                st.session_state.mg_num_manual_clips[mg_def_key] = 1
                                            mg_def_num_clips = st.number_input(
                                                "Number of defense clips (1-6)",
                                                min_value=1, max_value=6,
                                                value=st.session_state.mg_num_manual_clips[mg_def_key],
                                                key="mg_num_clips_backfoot_drive_def"
                                            )
                                            st.session_state.mg_num_manual_clips[mg_def_key] = mg_def_num_clips

                                        mg_def_cl_skey = "mg_backfoot_drive_def_clip_length"
                                        if mg_def_cl_skey not in st.session_state:
                                            st.session_state[mg_def_cl_skey] = 3
                                        mg_def_clip_len = st.number_input(
                                            "Defense Clip Length (seconds)",
                                            min_value=1,
                                            max_value=max(1, int(mg_def_dur)),
                                            value=st.session_state[mg_def_cl_skey],
                                            step=1,
                                            key="mg_clip_length_backfoot_drive_def"
                                        )
                                        st.session_state[mg_def_cl_skey] = mg_def_clip_len
                                        st.info(f"📏 All defense clips will be {mg_def_clip_len} seconds long")

                                        if mg_def_key not in st.session_state.mg_manual_clip_times:
                                            st.session_state.mg_manual_clip_times[mg_def_key] = []
                                        while len(st.session_state.mg_manual_clip_times[mg_def_key]) < mg_def_num_clips:
                                            st.session_state.mg_manual_clip_times[mg_def_key].append({'start': 0, 'end': mg_def_clip_len})
                                        while len(st.session_state.mg_manual_clip_times[mg_def_key]) > mg_def_num_clips:
                                            st.session_state.mg_manual_clip_times[mg_def_key].pop()

                                        mg_def_all_valid = True
                                        for mg_def_row in range(0, mg_def_num_clips, 6):
                                            mg_def_cols = st.columns(6)
                                            for mg_def_ci in range(6):
                                                mg_def_i = mg_def_row + mg_def_ci
                                                if mg_def_i >= mg_def_num_clips:
                                                    break
                                                with mg_def_cols[mg_def_ci]:
                                                    mg_def_max_s = max(0, int(mg_def_dur) - mg_def_clip_len)
                                                    mg_def_start = st.number_input(
                                                        f"Def Clip {mg_def_i+1} Start (s)",
                                                        min_value=0,
                                                        max_value=mg_def_max_s,
                                                        value=int(st.session_state.mg_manual_clip_times[mg_def_key][mg_def_i]['start']),
                                                        step=1,
                                                        key=f"mg_start_backfoot_drive_def_{mg_def_i}"
                                                    )
                                                    mg_def_end = mg_def_start + mg_def_clip_len
                                                    st.session_state.mg_manual_clip_times[mg_def_key][mg_def_i]['start'] = float(mg_def_start)
                                                    st.session_state.mg_manual_clip_times[mg_def_key][mg_def_i]['end'] = float(mg_def_end)
                                                    if mg_def_end > mg_def_dur:
                                                        st.error("❌ Exceeds duration")
                                                        mg_def_all_valid = False
                                                    else:
                                                        st.caption(f"→ {mg_def_end}s")

                                        mg_def_cr_col1, _ = st.columns([1, 3])
                                        with mg_def_cr_col1:
                                            mg_def_create_btn = st.button(
                                                "💾 Create Defense Clips",
                                                key="mg_create_clips_backfoot_drive_def",
                                                use_container_width=True,
                                                disabled=not mg_def_all_valid or st.session_state.mg_manual_clips_created.get(mg_def_key, False)
                                            )

                                        if mg_def_create_btn and mg_def_all_valid:
                                            with st.spinner(f"Creating {mg_def_num_clips} defense clips..."):
                                                try:
                                                    mg_def_pn = mg_player_name.lower().replace(' ', '_').replace('.', '_')
                                                    mg_def_created = []
                                                    mg_def_prog = st.progress(0)
                                                    mg_def_errs = []
                                                    for mg_def_i in range(mg_def_num_clips):
                                                        mg_def_st = st.session_state.mg_manual_clip_times[mg_def_key][mg_def_i]['start']
                                                        mg_def_en = st.session_state.mg_manual_clip_times[mg_def_key][mg_def_i]['end']
                                                        mg_def_cname = f"{mg_def_pn}_backfoot_defense_clip_{mg_def_i+1}.mp4"
                                                        mg_def_cb, mg_def_err = create_clip_from_video(
                                                            mg_def_info['bytes'],
                                                            mg_def_st, mg_def_en, mg_def_cname
                                                        )
                                                        if mg_def_cb:
                                                            mg_def_created.append({
                                                                'name': mg_def_cname,
                                                                'bytes': mg_def_cb,
                                                                'start': mg_def_st,
                                                                'end': mg_def_en,
                                                                'duration': mg_def_en - mg_def_st
                                                            })
                                                        else:
                                                            mg_def_errs.append(f"Clip {mg_def_i+1}: {mg_def_err}")
                                                            st.error(f"❌ Failed def clip {mg_def_i+1}: {mg_def_err}")
                                                        mg_def_prog.progress((mg_def_i + 1) / mg_def_num_clips)
                                                    mg_def_prog.empty()
                                                    if len(mg_def_created) == mg_def_num_clips:
                                                        st.session_state.mg_processed_clips[mg_def_key] = mg_def_created
                                                        st.session_state.mg_manual_clips_created[mg_def_key] = True
                                                        st.success(f"✅ Created {mg_def_num_clips} defense clips!")
                                                        st.rerun()
                                                    else:
                                                        st.error(f"❌ {len(mg_def_created)}/{mg_def_num_clips} clips created")
                                                        if mg_def_errs:
                                                            with st.expander("Error Details"):
                                                                for mg_de in mg_def_errs:
                                                                    st.write(f"• {mg_de}")
                                                except Exception as e:
                                                    st.error(f"Error creating defense clips: {e}")

                                        if st.session_state.mg_manual_clips_created.get(mg_def_key, False) and mg_def_key in st.session_state.mg_processed_clips:
                                            st.subheader("📹 Created Defense Clips")
                                            for mg_dr in range(0, len(st.session_state.mg_processed_clips[mg_def_key]), 3):
                                                mg_dc_cols = st.columns(3)
                                                for mg_dci, mg_dcd in enumerate(st.session_state.mg_processed_clips[mg_def_key][mg_dr:mg_dr+3]):
                                                    with mg_dc_cols[mg_dci]:
                                                        st.write(f"**Defense Clip {mg_dr + mg_dci + 1}**")
                                                        if 'start' in mg_dcd:
                                                            st.write(f"Start: {mg_dcd['start']:.2f}s | End: {mg_dcd['end']:.2f}s")
                                                        st.video(mg_dcd['bytes'])

                                            mg_def_up_col1, _ = st.columns([1, 3])
                                            with mg_def_up_col1:
                                                mg_def_upload_btn = st.button(
                                                    "☁️ Upload Defense to GCS",
                                                    key="mg_upload_backfoot_drive_def",
                                                    use_container_width=True,
                                                    disabled=mg_def_key in st.session_state.mg_signed_urls
                                                )

                                            if mg_def_upload_btn:
                                                mg_def_all_idx = list(range(len(st.session_state.mg_processed_clips[mg_def_key])))
                                                mg_def_surls, mg_def_up_err = upload_selected_clips_to_gcs(
                                                    mg_assessment_id,
                                                    mg_player_name,
                                                    'backfoot_defense',
                                                    mg_def_all_idx,
                                                    st.session_state.mg_processed_clips[mg_def_key]
                                                )
                                                if mg_def_up_err:
                                                    st.error(mg_def_up_err)
                                                else:
                                                    st.session_state.mg_signed_urls[mg_def_key] = mg_def_surls
                                                    st.success(f"✅ Uploaded {len(mg_def_surls)} defense clips to GCS!")

                                            if mg_def_key in st.session_state.mg_signed_urls:
                                                mg_def_urls = st.session_state.mg_signed_urls[mg_def_key]
                                                st.success(f"✅ {len(mg_def_urls)} defense clips ready")

                                # ── Flyte Task ──
                                st.divider()
                                mg_default_fb = st.session_state.mg_saved_coach_feedback.get(f"{mg_assessment_id}_{mg_drill_type}", '')
                                mg_flyte_fb = st.text_area(
                                    "Coach Feedback for Task",
                                    value=mg_default_fb,
                                    key=f"mg_flyte_fb_{mg_drill_type}",
                                    placeholder="Enter coach feedback..."
                                )
                                if mg_default_fb:
                                    st.info("ℹ️ Coach feedback auto-populated from saved feedback")

                                mg_flyte_inputs = build_flyte_inputs(
                                    mg_drill_type,
                                    mg_player_name,
                                    mg_assessment_id,
                                    st.session_state.mg_signed_urls[mg_drill_type],
                                    mg_flyte_fb,
                                    mg_def_urls
                                )

                                if mg_drill_type == 'backfoot_drive':
                                    if mg_def_urls:
                                        st.info(f"ℹ️ Task includes {len(st.session_state.mg_signed_urls[mg_drill_type])} drive clips and {len(mg_def_urls)} defense clips")
                                    else:
                                        st.info(f"ℹ️ Task includes {len(st.session_state.mg_signed_urls[mg_drill_type])} drive clips (no defense clips)")

                                st.write("**Flyte Task Inputs:**")
                                st.json(mg_flyte_inputs, expanded=False)

                                mg_run_col1, _ = st.columns([1, 3])
                                with mg_run_col1:
                                    mg_run_btn = st.button(
                                        "▶️ Run Task",
                                        key=f"mg_run_{mg_drill_type}",
                                        use_container_width=True,
                                        disabled=mg_drill_type in st.session_state.mg_execution_ids
                                    )

                                if mg_run_btn:
                                    with st.spinner("Submitting task..."):
                                        try:
                                            mg_resp = run_task(mg_flyte_inputs)
                                            mg_exec_id = mg_resp.get("execution_id")
                                            st.session_state.mg_execution_ids[mg_drill_type] = mg_exec_id
                                            st.session_state.mg_task_status[mg_drill_type] = {
                                                "execution_id": mg_exec_id,
                                                "status": "submitted",
                                                "is_done": False
                                            }
                                            st.success(f"✅ Task submitted! Execution ID: {mg_exec_id}")
                                        except Exception as e:
                                            st.error(f"Error: {e}")

                                if mg_drill_type in st.session_state.mg_execution_ids:
                                    st.divider()
                                    st.subheader("📊 Task Status")
                                    mg_exec_id = st.session_state.mg_execution_ids[mg_drill_type]
                                    st.write(f"**Execution ID:** `{mg_exec_id}`")

                                    mg_st_col1, _ = st.columns([1, 3])
                                    with mg_st_col1:
                                        mg_check_btn = st.button(
                                            "🔄 Check Status",
                                            key=f"mg_status_{mg_drill_type}",
                                            use_container_width=True
                                        )

                                    if mg_check_btn:
                                        with st.spinner("Fetching status..."):
                                            try:
                                                mg_st_resp = fetch_task_execution_post({"execution_id": mg_exec_id})
                                                st.session_state.mg_task_status[mg_drill_type] = mg_st_resp
                                            except Exception as e:
                                                st.error(f"Error: {e}")

                                    if mg_drill_type in st.session_state.mg_task_status:
                                        mg_st_data = st.session_state.mg_task_status[mg_drill_type]
                                        mg_is_done = mg_st_data.get("is_done", False)
                                        if mg_is_done:
                                            st.success("✅ Task Completed!")
                                        else:
                                            st.info("⏳ Task In Progress...")
                                        with st.expander("View Full Status", expanded=mg_is_done):
                                            st.json(mg_st_data)

                                        # Metrics editor — drive drills only
                                        if mg_is_done and mg_drill_type in ['tophand', 'bottomhand', 'backfoot_drive', 'backfoot_defense']:
                                            st.divider()
                                            st.subheader("✏️ Edit Metrics")

                                            mg_metrics_key = f"{mg_assessment_id}_{mg_drill_type}"

                                            if mg_metrics_key not in st.session_state.mg_drill_metrics_data:
                                                with st.spinner("Loading drill metrics from MongoDB..."):
                                                    mg_drill_doc = fetch_drill_results(mg_assessment_id)

                                                    if mg_drill_doc and 'drill_metrics' in mg_drill_doc:
                                                        mg_all_metrics = mg_drill_doc['drill_metrics']
                                                        mg_avail_drills = list(mg_all_metrics.keys())
                                                        st.info(f"📋 Available drills in MongoDB: {', '.join(mg_avail_drills)}")

                                                        mg_mongo_key = get_mongo_drill_key(mg_drill_type)
                                                        mg_possible_keys = [mg_mongo_key, mg_drill_type]
                                                        mg_found_key = None

                                                        for mg_k in mg_possible_keys:
                                                            if mg_k in mg_all_metrics:
                                                                mg_found_key = mg_k
                                                                break

                                                        if mg_found_key:
                                                            st.session_state.mg_drill_metrics_data[mg_metrics_key] = mg_all_metrics[mg_found_key]
                                                            st.session_state.mg_mongo_drill_keys[mg_metrics_key] = mg_found_key

                                                            mg_m = mg_all_metrics[mg_found_key]
                                                            mg_shot_angles = mg_m.get('shot_direction_angles', [])
                                                            mg_ball_dirs = mg_m.get('ball_directions', [])

                                                            if not mg_shot_angles or not mg_ball_dirs:
                                                                st.error(f"⚠️ Drill '{mg_found_key}' has no shot_direction_angles or ball_directions data")
                                                            else:
                                                                st.session_state.mg_edited_angles[mg_metrics_key] = mg_shot_angles.copy()
                                                                st.session_state.mg_edited_directions[mg_metrics_key] = mg_ball_dirs.copy()
                                                                st.session_state.mg_original_values[mg_metrics_key] = {
                                                                    'angles': mg_shot_angles.copy(),
                                                                    'directions': mg_ball_dirs.copy()
                                                                }
                                                        else:
                                                            st.warning(f"❌ No metrics found for '{mg_drill_type}' (tried: {', '.join(mg_possible_keys)})")
                                                            st.write("**Available drill types:**")
                                                            for mg_ad in mg_avail_drills:
                                                                st.write(f"- {mg_ad}")
                                                    else:
                                                        st.warning("No drill results found in MongoDB for this assessment")

                                            if mg_metrics_key in st.session_state.mg_drill_metrics_data:
                                                mg_metrics = st.session_state.mg_drill_metrics_data[mg_metrics_key]
                                                mg_mongo_drill_key = st.session_state.mg_mongo_drill_keys.get(mg_metrics_key, mg_drill_type)

                                                # Detect batter hand
                                                mg_batter_hand = 'Right'
                                                if 'new_metrics' in mg_metrics and 'batter_hand' in mg_metrics['new_metrics']:
                                                    mg_bh_list = mg_metrics['new_metrics']['batter_hand']
                                                    if mg_bh_list:
                                                        mg_batter_hand = max(set(mg_bh_list), key=mg_bh_list.count)

                                                st.write(f"**Batter Hand:** {mg_batter_hand}")
                                                st.write(f"**MongoDB Drill Key:** `{mg_mongo_drill_key}`")

                                                mg_ball_dir_opts = ['Off Stump', 'Outside Off Stump', 'Middle Stump', 'Leg Stump']
                                                mg_num_shots = len(st.session_state.mg_edited_angles[mg_metrics_key])
                                                st.write(f"**Number of Shots:** {mg_num_shots}")
                                                st.write("**Edit Shot Direction Angles and Ball Directions:**")

                                                mg_hc1, mg_hc2, mg_hc3 = st.columns([1, 2, 2])
                                                with mg_hc1:
                                                    st.write("**Shot #**")
                                                with mg_hc2:
                                                    st.write("**Shot Direction Angle**")
                                                with mg_hc3:
                                                    st.write("**Ball Direction**")

                                                for mg_si in range(mg_num_shots):
                                                    mg_c1, mg_c2, mg_c3 = st.columns([1, 2, 2])
                                                    with mg_c1:
                                                        st.write(f"Shot {mg_si+1}")
                                                    with mg_c2:
                                                        mg_cur_angle = st.session_state.mg_edited_angles[mg_metrics_key][mg_si]
                                                        mg_new_angle = st.number_input(
                                                            f"Angle {mg_si+1}",
                                                            value=float(mg_cur_angle),
                                                            step=1.0,
                                                            key=f"mg_angle_{mg_drill_type}_{mg_si}",
                                                            label_visibility="collapsed"
                                                        )
                                                        st.session_state.mg_edited_angles[mg_metrics_key][mg_si] = mg_new_angle
                                                    with mg_c3:
                                                        mg_cur_dir = st.session_state.mg_edited_directions[mg_metrics_key][mg_si]
                                                        mg_dir_idx = mg_ball_dir_opts.index(mg_cur_dir) if mg_cur_dir in mg_ball_dir_opts else 0
                                                        mg_new_dir = st.selectbox(
                                                            f"Ball Direction {mg_si+1}",
                                                            options=mg_ball_dir_opts,
                                                            index=mg_dir_idx,
                                                            key=f"mg_direction_{mg_drill_type}_{mg_si}",
                                                            label_visibility="collapsed"
                                                        )
                                                        st.session_state.mg_edited_directions[mg_metrics_key][mg_si] = mg_new_dir

                                                mg_angles_changed = st.session_state.mg_edited_angles[mg_metrics_key] != st.session_state.mg_original_values[mg_metrics_key]['angles']
                                                mg_dirs_changed = st.session_state.mg_edited_directions[mg_metrics_key] != st.session_state.mg_original_values[mg_metrics_key]['directions']
                                                mg_vals_changed = mg_angles_changed or mg_dirs_changed

                                                mg_upd_col1, _, _ = st.columns([1, 2, 2])
                                                with mg_upd_col1:
                                                    mg_update_btn = st.button(
                                                        "💾 Update Metrics",
                                                        key=f"mg_update_{mg_drill_type}",
                                                        disabled=not mg_vals_changed,
                                                        use_container_width=True
                                                    )

                                                if mg_vals_changed:
                                                    st.info("⚠️ You have unsaved changes")

                                                if mg_update_btn:
                                                    with st.spinner("Updating metrics..."):
                                                        try:
                                                            mg_updated = recalculate_metrics(
                                                                mg_metrics,
                                                                st.session_state.mg_edited_angles[mg_metrics_key],
                                                                st.session_state.mg_edited_directions[mg_metrics_key],
                                                                mg_batter_hand
                                                            )

                                                            mg_upd_success = update_drill_results_in_mongo(
                                                                mg_assessment_id,
                                                                mg_mongo_drill_key,
                                                                mg_updated
                                                            )

                                                            if mg_upd_success:
                                                                st.success("✅ Metrics updated successfully in MongoDB!")

                                                                st.session_state.mg_drill_metrics_data[mg_metrics_key] = mg_updated
                                                                st.session_state.mg_original_values[mg_metrics_key] = {
                                                                    'angles': st.session_state.mg_edited_angles[mg_metrics_key].copy(),
                                                                    'directions': st.session_state.mg_edited_directions[mg_metrics_key].copy()
                                                                }

                                                                st.divider()
                                                                st.subheader("📊 Updated Metrics")
                                                                with st.expander("View Updated MongoDB Document", expanded=True):
                                                                    st.json(mg_updated)

                                                                mg_mc1, mg_mc2, mg_mc3 = st.columns(3)
                                                                with mg_mc1:
                                                                    st.metric("Grade", mg_updated.get('grade', 'N/A'))
                                                                with mg_mc2:
                                                                    st.metric("Percentile", f"{mg_updated.get('percentile', 0)}%")
                                                                with mg_mc3:
                                                                    if 'new_metrics' in mg_updated and 'score' in mg_updated['new_metrics']:
                                                                        mg_avg = sum(mg_updated['new_metrics']['score']) / len(mg_updated['new_metrics']['score'])
                                                                        st.metric("Avg Score", f"{mg_avg:.2f}")

                                                                st.rerun()
                                                            else:
                                                                st.error(f"❌ Failed to update metrics — no document found with assessment_id=`{mg_assessment_id}` in MongoDB. Check the console logs for details.")

                                                        except Exception as e:
                                                            st.error(f"Error updating metrics: {str(e)}")
                                                            st.exception(e)

# ============================================================================
# SIDEBAR
# ============================================================================
with st.sidebar:
    st.header("ℹ️ About")
    st.write("""
    This app allows you to:
    1. Search for assessments by ID
    2. View player information
    3. Browse available drill types
    4. **Choose clip mode**:
       - ✂️ Manual Clipping: Create custom clips from raw video
       - 📹 Auto Generated: Select from pre-processed clips
    5. View and select drill clips (cached for performance)
    6. Add and save coach feedback (before clip selection)
    7. Upload selected clips to GCS
    8. Run Flyte tasks with custom inputs (auto-populates saved feedback)
    9. Monitor task execution status
    10. Edit shot angles and ball directions (drive drills)
    11. Auto-recalculate metrics and save to MongoDB
    
    ---
    **🎬 Manual Generation** tab:
    - Enter any assessment ID and player name
    - Upload video directly for any drill type
    - Full clipping, blur, GCS upload & Flyte flow
    """)
    
    if st.session_state.searched:
        st.divider()
        st.subheader("Current Session")
        st.write(f"**Player:** {st.session_state.player_name}")
        if st.session_state.selected_drill:
            drill_type = st.session_state.selected_drill
            st.write(f"**Selected Drill:** {drill_type}")
            drill_mode = st.session_state.clip_selection_mode.get(drill_type, 'None')
            if drill_mode == 'manual':
                st.write(f"**Mode:** ✂️ Manual Clipping")
            elif drill_mode == 'auto':
                st.write(f"**Mode:** 📹 Auto-Generated")
            else:
                st.write(f"**Mode:** Not selected")
            if drill_type in st.session_state.processed_clips:
                st.write(f"**Clips:** {len(st.session_state.processed_clips[drill_type])}")
            else:
                st.write(f"**Clips:** 0")
        
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
        num_old_cached = len(st.session_state.clips_cache)
        num_auto_cached = len(st.session_state.auto_clips_cache)
        num_manual_cached = len(st.session_state.manual_clips_cache)
        total_cached = num_old_cached + num_auto_cached + num_manual_cached
        
        st.write(f"**Total Cached:** {total_cached}")
        if num_auto_cached > 0:
            st.write(f"- Auto clips: {num_auto_cached}")
        if num_manual_cached > 0:
            st.write(f"- Manual clips: {num_manual_cached}")
        if num_old_cached > 0:
            st.write(f"- Legacy cache: {num_old_cached}")

    if st.session_state.mg_assessment_started:
        st.divider()
        st.subheader("Manual Generation")
        st.write(f"**Player:** {st.session_state.mg_player_name_val}")
        st.write(f"**Assessment:** `{st.session_state.mg_assessment_id_val}`")
        mg_tasks = len(st.session_state.mg_execution_ids)
        st.write(f"**Tasks:** {mg_tasks}")
        if mg_tasks > 0:
            for mg_d, mg_eid in st.session_state.mg_execution_ids.items():
                mg_se = "✅" if st.session_state.mg_task_status.get(mg_d, {}).get("is_done", False) else "⏳"
                st.write(f"{mg_se} {mg_d}")

    st.divider()
    if st.button("🗑️ Clear Cache & Reset", use_container_width=True):
        st.session_state.searched = False
        st.session_state.player_name = None
        st.session_state.drill_data = {}
        st.session_state.selected_drill = None
        st.session_state.processed_clips = {}
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
        st.session_state.manual_clipping_mode = {}
        st.session_state.raw_video_data = {}
        st.session_state.num_manual_clips = {}
        st.session_state.manual_clip_times = {}
        st.session_state.manual_clips_created = {}
        st.session_state.clip_selection_mode = {}
        st.session_state.auto_clips_cache = {}
        st.session_state.manual_clips_cache = {}
        st.session_state.blur_masks = {}
        st.session_state.blur_levels = {}
        st.session_state.blur_history = {}
        st.session_state.blur_enabled = {}
        # Reset manual generation state
        st.session_state.mg_assessment_started = False
        st.session_state.mg_assessment_id_val = ''
        st.session_state.mg_player_name_val = ''
        st.session_state.mg_raw_video_data = {}
        st.session_state.mg_num_manual_clips = {}
        st.session_state.mg_manual_clip_times = {}
        st.session_state.mg_manual_clips_created = {}
        st.session_state.mg_processed_clips = {}
        st.session_state.mg_blur_masks = {}
        st.session_state.mg_blur_levels = {}
        st.session_state.mg_blur_history = {}
        st.session_state.mg_blur_enabled = {}
        st.session_state.mg_signed_urls = {}
        st.session_state.mg_execution_ids = {}
        st.session_state.mg_task_status = {}
        st.session_state.mg_saved_coach_feedback = {}
        st.session_state.mg_running_drill_inputs = {}
        st.session_state.mg_drill_metrics_data = {}
        st.session_state.mg_edited_angles = {}
        st.session_state.mg_edited_directions = {}
        st.session_state.mg_original_values = {}
        st.session_state.mg_mongo_drill_keys = {}
        st.success("Cache cleared!")
        st.rerun()

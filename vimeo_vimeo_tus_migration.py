import requests
import json
import time
import os
import urllib3

# Suppress the InsecureRequestWarning when verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
# Your Source Vimeo Credentials and Folder ID
SOURCE_VIMEO_ACCESS_TOKEN = "YOUR_SOURCE_ACCESS_TOKEN"
SOURCE_VIMEO_FOLDER_ID = "YOUR_SOURCE_FOLDER_ID"

# Your Destination Vimeo Credentials and Folder ID
DESTINATION_VIMEO_ACCESS_TOKEN = "YOUR_DESTINATION_ACCESS_TOKEN"
DESTINATION_VIMEO_FOLDER_ID = "YOUR_DESTINATION_FOLDER_ID"

# Folder to temporarily store downloaded videos
DOWNLOAD_FOLDER = "temp_vimeo_downloads"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# --- Vimeo API Functions ---
def get_videos_from_folder(access_token, folder_id):
    """
    Retrieves a complete list of video URIs and names from a specified folder, handling pagination and sorting.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.vimeo.*+json;version=3.4"
    }
    
    videos = []
    page_url = f"https://api.vimeo.com/me/projects/{folder_id}/videos?per_page=100&sort=alphabetical&direction=asc"
    
    while page_url:
        try:
            response = requests.get(page_url, headers=headers, verify=False)
            response.raise_for_status()
            response_data = response.json()
            
            videos.extend(response_data.get('data', []))
            
            page_url = response_data.get('paging', {}).get('next')
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting videos from source folder {folder_id}: {e}")
            return None
            
    return videos

def get_video_download_url_and_title(access_token, video_uri):
    """
    Retrieves the direct download URL for the highest quality MP4 file and the video title.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.vimeo.*+json;version=3.4"
    }
    
    url = f"https://api.vimeo.com{video_uri}"
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        video_data = response.json()
        
        download_links = video_data.get('download', [])
        if not download_links:
            print(f"  -> No download links found for {video_uri}.")
            return None, None
        
        download_links.sort(key=lambda x: int(x.get('width', 0)), reverse=True)
        best_link = download_links[0]
        
        return video_data.get('name'), best_link.get('link')

    except requests.exceptions.RequestException as e:
        print(f"Error getting video details for {video_uri}: {e}")
        return None, None

def download_video(download_url, title):
    """
    Downloads a video from the given URL and saves it locally.
    """
    headers = {
        "Authorization": f"Bearer {SOURCE_VIMEO_ACCESS_TOKEN}",
        "Accept": "application/vnd.vimeo.*+json;version=3.4"
    }
    
    filename = os.path.join(DOWNLOAD_FOLDER, f"{title.replace(' ', '_')}.mp4")
    
    print(f"  -> Downloading '{title}' to {filename}...")
    try:
        with requests.get(download_url, headers=headers, stream=True, verify=False) as r:
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"  -> Download of '{title}' complete.")
        return filename
    except requests.exceptions.RequestException as e:
        print(f"  -> Failed to download video '{title}': {e}")
        return None

def upload_video_to_vimeo(access_token, filepath, title):
    """
    Uploads a video from a local file to Vimeo and moves it to a folder.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.vimeo.*+json;version=3.4",
        "Content-Type": "application/json"
    }
    
    upload_data = {
        "upload": {
            "approach": "tus",
            "size": os.path.getsize(filepath)
        },
        "name": title
    }
    
    try:
        print(f"  -> Starting upload for '{title}'...")
        post_response = requests.post("https://api.vimeo.com/me/videos", headers=headers, json=upload_data, verify=False)
        post_response.raise_for_status()
        
        vimeo_video_data = post_response.json()
        upload_link = vimeo_video_data['upload']['upload_link']
        
    except requests.exceptions.RequestException as e:
        print(f"  -> Failed to get upload ticket for '{title}': {e}")
        return None

    upload_headers = {
        "Tus-Resumable": "1.0.0",
        "Upload-Offset": "0",
        "Content-Type": "application/offset+octet-stream"
    }
    
    try:
        with open(filepath, 'rb') as f:
            put_response = requests.patch(upload_link, headers=upload_headers, data=f, verify=False)
            put_response.raise_for_status()
        print(f"  -> Upload of '{title}' complete.")
        
        new_video_uri = vimeo_video_data.get('uri')
        new_video_id = new_video_uri.split('/')[-1]

    except requests.exceptions.RequestException as e:
        print(f"  -> Failed to upload file for '{title}': {e}")
        return None
    
    try:
        print(f"  -> Moving video {new_video_id} to folder {DESTINATION_VIMEO_FOLDER_ID}...")
        move_url = f"https://api.vimeo.com/me/projects/{DESTINATION_VIMEO_FOLDER_ID}/videos/{new_video_id}"
        put_response = requests.put(move_url, headers=headers, verify=False)
        put_response.raise_for_status()

        print(f"  -> Success: Video '{title}' moved to folder {DESTINATION_VIMEO_FOLDER_ID}.")
        
    except requests.exceptions.RequestException as e:
        print(f"  -> Failed to move video '{title}' to folder {DESTINATION_VIMEO_FOLDER_ID}: {e}")

    return new_video_uri

# --- Main Migration Logic ---
if __name__ == "__main__":
    print("Starting Vimeo to Vimeo migration (Download/Upload method) with pagination and sorting...")
    print("Warning: SSL certificate verification has been disabled for this script.")

    source_videos = get_videos_from_folder(SOURCE_VIMEO_ACCESS_TOKEN, SOURCE_VIMEO_FOLDER_ID)
    if not source_videos:
        print("Failed to get videos from source folder. Exiting.")
        exit(1)
    
    print(f"Found {len(source_videos)} videos in the source folder.")
    
    for video in source_videos:
        video_uri = video.get('uri')
        video_name = video.get('name')
        if not video_uri or not video_name:
            print("Skipping a video due to missing URI or name.")
            continue
            
        print(f"\nProcessing source video: {video_uri}...")
        
        title, download_url = get_video_download_url_and_title(SOURCE_VIMEO_ACCESS_TOKEN, video_uri)
        
        if not title or not download_url:
            print(f"Skipping video {video_uri} due to missing data.")
            continue

        temp_file_path = download_video(download_url, title)
        
        if not temp_file_path:
            continue
            
        upload_video_to_vimeo(DESTINATION_VIMEO_ACCESS_TOKEN, temp_file_path, title)
        
        try:
            os.remove(temp_file_path)
            print(f"  -> Removed temporary file: {temp_file_path}")
        except OSError as e:
            print(f"Error removing temporary file {temp_file_path}: {e}")

    print("\nMigration script completed.")

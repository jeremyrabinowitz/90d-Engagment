import os
import requests
from datetime import datetime, timedelta, timezone
from dateutil import parser
import isodate

# CONFIG
# These environment variables will be loaded from the Render.com environment
# as they are set in the Render UI.
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = 'Influencers'
AIRTABLE_VIEW_NAME = 'RM Engagement Data'
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')

# API Endpoints
AIRTABLE_ENDPOINT = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

YOUTUBE_CHANNELS_ENDPOINT = "https://www.googleapis.com/youtube/v3/channels"
YOUTUBE_PLAYLIST_ITEMS_ENDPOINT = "https://www.googleapis.com/youtube/v3/playlistItems"
YOUTUBE_VIDEOS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"

# Set cutoff date for videos in the last 90 days
CUTOFF_DATE = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=90)

# Error handling configuration
ERROR_LIMIT = 10
error_count = 0


def get_airtable_records():
    """
    Fetches all records from the specified Airtable view, handling pagination.
    """
    records = []
    offset = None
    while True:
        params = {"view": AIRTABLE_VIEW_NAME}
        if offset:
            params['offset'] = offset
        
        try:
            resp = requests.get(AIRTABLE_ENDPOINT, headers=HEADERS, params=params)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get('records', []))
            offset = data.get('offset')
            if not offset:
                break
        except requests.RequestException as e:
            print(f"Error fetching Airtable records: {e}")
            break
    return records


def get_uploads_playlist_id(channel_id):
    """
    Retrieves the 'uploads' playlist ID for a given YouTube channel ID.
    """
    global error_count
    url = YOUTUBE_CHANNELS_ENDPOINT
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": YOUTUBE_API_KEY
    }
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        items = resp.json().get('items', [])
        if not items:
            return None
        return items[0]['contentDetails']['relatedPlaylists']['uploads']
    except requests.RequestException as e:
        error_count += 1
        print(f"Error getting uploads playlist ID for channel {channel_id}: {e}")
        return None


def get_recent_video_ids(playlist_id):
    """
    Fetches video IDs from a playlist that were published within the CUTOFF_DATE.
    """
    global error_count
    video_ids = []
    next_page = None
    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "key": YOUTUBE_API_KEY
        }
        if next_page:
            params['pageToken'] = next_page
        
        try:
            resp = requests.get(YOUTUBE_PLAYLIST_ITEMS_ENDPOINT, params=params)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            error_count += 1
            print(f"Error fetching recent video IDs from playlist {playlist_id}: {e}")
            break

        recent_videos_found_in_page = False
        for item in data.get('items', []):
            try:
                published_at = parser.parse(item['contentDetails']['videoPublishedAt'])
                if published_at >= CUTOFF_DATE:
                    video_ids.append(item['contentDetails']['videoId'])
                    recent_videos_found_in_page = True
            except KeyError:
                continue

        if not recent_videos_found_in_page and data.get('items'):
            break

        next_page = data.get('nextPageToken')
        if not next_page:
            break

    return video_ids


def get_video_stats_batch(video_ids):
    """
    Fetches contentDetails and statistics for a batch of video IDs.
    """
    global error_count
    stats_data = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        params = {
            "part": "contentDetails,statistics",
            "id": ",".join(batch),
            "key": YOUTUBE_API_KEY
        }
        try:
            resp = requests.get(YOUTUBE_VIDEOS_ENDPOINT, params=params)
            resp.raise_for_status()
            stats_data.extend(resp.json().get('items', []))
        except requests.RequestException as e:
            error_count += 1
            print(f"Error getting video stats for batch {batch}: {e}")
            break
    return stats_data


def is_longform(iso_duration):
    """
    Checks if a video's duration is 3 minutes (180 seconds) or longer.
    """
    duration = isodate.parse_duration(iso_duration)
    return duration.total_seconds() >= 180


def update_airtable_record(record_id, fields_to_update):
    """
    Updates the specified Airtable record with multiple fields in a single call.
    """
    url = f"{AIRTABLE_ENDPOINT}/{record_id}"
    payload = {
        "fields": fields_to_update
    }
    try:
        resp = requests.patch(url, headers=HEADERS, json=payload)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Error updating Airtable record {record_id}: {e}")


def main():
    """
    Main function to orchestrate the fetching and updating process.
    """
    print("Starting combined YouTube data update process...")
    
    # Check if environment variables are set before proceeding
    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, YOUTUBE_API_KEY]):
        print("❌ Error: Missing one or more environment variables (AIRTABLE_API_KEY, AIRTABLE_BASE_ID, YOUTUBE_API_KEY). Please set them in the Render UI.")
        return

    # --- 1. Fetch Airtable Records ---
    records = get_airtable_records()
    if not records:
        print("No records found in Airtable to process.")
        return

    updated_count = 0
    
    for record in records:
        if error_count >= ERROR_LIMIT:
            print("❌ Too many YouTube API errors — stopping execution.")
            break

        channel_id = record['fields'].get('YouTube Channel ID')
        if not channel_id:
            print(f"Skipping record {record['id']}: No YouTube Channel ID found.")
            continue

        print(f"Processing channel ID: {channel_id} from record {record['id']}")

        # --- 2. Get Uploads Playlist ID ---
        playlist_id = get_uploads_playlist_id(channel_id)
        if not playlist_id:
            print(f"Could not get uploads playlist for channel {channel_id}.")
            update_airtable_record(record['id'], {"LGVPV90": 0, "LGLPV90": 0, "LGCPV90": 0})
            updated_count += 1
            continue

        # --- 3. Find Recent Video IDs ---
        video_ids = get_recent_video_ids(playlist_id)
        if not video_ids:
            print(f"No recent videos found for channel {channel_id} in the last 90 days.")
            update_airtable_record(record['id'], {"LGVPV90": 0, "LGLPV90": 0, "LGCPV90": 0})
            updated_count += 1
            continue
        
        print(f"Found {len(video_ids)} recent videos for channel {channel_id}.")

        # --- 4. Fetch All Stats in Batches ---
        video_stats_list = get_video_stats_batch(video_ids)
        
        # --- 5. Filter for Long-Form and collect stats ---
        longform_views = []
        longform_likes = []
        longform_comments = []

        for item in video_stats_list:
            try:
                duration = item['contentDetails']['duration']
                if is_longform(duration):
                    views = int(item['statistics'].get('viewCount', 0))
                    likes = int(item['statistics'].get('likeCount', 0))
                    comments = int(item['statistics'].get('commentCount', 0))
                    
                    longform_views.append(views)
                    longform_likes.append(likes)
                    longform_comments.append(comments)
            except KeyError:
                continue

        # --- 6. Calculate Averages ---
        avg_views = int(sum(longform_views) / len(longform_views)) if longform_views else 0
        avg_likes = int(sum(longform_likes) / len(longform_likes)) if longform_likes else 0
        avg_comments = int(sum(longform_comments) / len(longform_comments)) if longform_comments else 0

        # --- 7. Update Airtable Record with all metrics ---
        fields_to_update = {
            "LGVPV90": avg_views,
            "LGLPV90": avg_likes,
            "LGCPV90": avg_comments
        }
        update_airtable_record(record['id'], fields_to_update)
        
        print(f"Updated record {record['id']} with Views: {avg_views}, Likes: {avg_likes}, Comments: {avg_comments}")
        updated_count += 1

    print(f"✅ Total records updated: {updated_count}")
    print("Combined data update process finished.")


if __name__ == "__main__":
    main()

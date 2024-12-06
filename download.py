import csv
import os
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to download the video using youtube-dl and extract the segment
def download_and_extract_video(video_url, start_time, output_dir, video_id):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if os.path.exists(os.path.join(output_dir, video_id)):
        return
    complete_video_filename = os.path.join(output_dir, video_id, f"{video_id}_video_complete.mp4")
    video_filename = os.path.join(output_dir, video_id, f"{video_id}_video.mp4")
    audio_filename = os.path.join(output_dir, video_id, f"{video_id}_audio.wav")

    # Download the video with youtube-dl, starting from the specified start time
    download_command = [
        "youtube-dl",
        video_url,
        "-f", "best",  # Download the best video quality
        "--output", complete_video_filename,
        "--user-agent", "Mozilla/5.0 (Android 14; Mobile; rv:128.0) Gecko/128.0 Firefox/128.0"
    ]
    ffmpeg_video_command = [
        "ffmpeg",
        "-i", complete_video_filename,  # Input video file
        "-ss", start_time,      # Start time for the trim
        "-t", "30",             # Duration of the clip (10 seconds)
        "-c:v", "libx264",      # Copy the video stream without re-encoding
        "-c:a", "copy",         # Copy the audio stream without re-encoding
        video_filename ,        # Output file name
    ]

    subprocess.run(download_command, check=False)
    print(f"Video downloaded to {complete_video_filename}")

    # Run the ffmpeg command
    subprocess.run(ffmpeg_video_command, check=False)
    print(f"Video trimmed and saved to {video_filename}")

    # Convert the downloaded video to WAV using ffmpeg
    ffmpeg_command = [
        "ffmpeg",
        "-i", video_filename,  # Input video file
        "-vn",  # Disable video (no need to re-encode video)
        "-acodec", "pcm_s16le",  # Use pcm_s16le codec for WAV
        "-ar", "16000",  # Set audio sample rate to 16000Hz
        "-ac", "2",  # Set stereo audio
        audio_filename,  # Output audio file
    ]

    subprocess.run(ffmpeg_command, check=False)
    print(f"Audio extracted and saved to {audio_filename}")

# Read CSV file and process each line in parallel
def process_csv(csv_filename, output_dir, max_downloads):
    cnt = 0
    max = 1000
    if max_downloads is not None:
        max = max_downloads

    # Initialize ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor() as executor:
        futures = []
        with open(csv_filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if cnt == 0:
                    cnt += 1
                    continue
                video_id = row[0]
                start_time = row[1]

                # Construct the video URL (assuming it's a YouTube video)
                video_url = f"https://www.youtube.com/watch?v={video_id}"

                print(f"Scheduling video {video_url} starting at {start_time}s...")
                # Submit the task to the executor for parallel execution
                futures.append(executor.submit(download_and_extract_video, video_url, start_time, output_dir, video_id))
                cnt += 1
                if cnt >= max:
                    break

        # Wait for all futures to complete and handle any exceptions
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {e}")

if __name__ == "__main__":
    csv_filename = "eval_file.csv"  # Path to your CSV file
    output_dir = "test_videos"  # Output directory for downloaded videos and audio
    parser = argparse.ArgumentParser(description="Download videos and audio from CSV.")
    parser.add_argument('--max-downloads', type=int, default=None, help="Maximum number of downloads to process.")
    args = parser.parse_args()
    process_csv(csv_filename, output_dir, args.max_downloads)

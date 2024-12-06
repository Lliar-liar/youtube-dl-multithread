## youtube-dl-multithread
Use youtube-dl to download datasets like VGG sound.
We use a user-agent so it's able to download while the original youtube-dl script don't.
In addition, we use ThreadPoolExecutor to support multithread download. It's now able to download 1000 videos in 10min.

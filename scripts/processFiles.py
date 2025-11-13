import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Iterable

from fileStreams import getFileJsonStream
from utils import FileProgressLog

import json
from datetime import datetime, timezone

# Hardcoded filters (adjust later to pass in with arguments)
START_DT = datetime(2025, 7, 4, 0, 0, 0, tzinfo=timezone.utc)
END_DT = datetime(2025, 7, 11, 23, 59, 59, tzinfo=timezone.utc)
SEARCH_TERMS = [
    "texas flood",
    "tx flood",
    "houston flood",
    "flash flood",
    "rain damage"
]
SEARCH_TERMS_LC = [t.lower() for t in SEARCH_TERMS]

# Output setup
OUTPUT_JSON_PATH = "filtered_posts.json"
matched_posts = []
_seen_ids = set()


def in_time_range(created_utc: float) -> bool:
    ts = float(created_utc)
    return START_DT.timestamp() <= ts <= END_DT.timestamp()


def match_term(row: dict) -> str | None:
    text = f"{row.get('title', '')} {row.get('selftext', '')}".lower()
    for term in SEARCH_TERMS_LC:
        if term in text:
            return term
    return None

# Change this path to the file or folder you want to process
fileOrFolderPath = r"E:/RedditDownloads/reddit/submissions/RS_2025-07.zst" 
recursive = False

def processFile(path: str):
    print(f"Processing file {path}")
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        progressLog = FileProgressLog(path, f)
        for row in jsonStream:
            progressLog.onRow()
            # PUT YOUR CODE HERE
            created = row.get("created_utc")
            if created is None or not in_time_range(created):
                continue

            matched = match_term(row)
            if not matched:
                continue

            sid = row.get("id")
            if not sid or sid in _seen_ids:
                continue
            _seen_ids.add(sid)

            created_iso = datetime.fromtimestamp(float(created)).isoformat()

            post_data = {
                "id": sid,
                "created_utc": created,
                "created_iso": created_iso,
                "author": row.get("author") or "[deleted]",
                "subreddit": row.get("subreddit"),
                "title": row.get("title"),
                "selftext": row.get("selftext"),
                "score": row.get("score"),
                "num_comments": row.get("num_comments"),
                "url": row.get("url"),
                "matched_term": matched,
            }

            matched_posts.append(post_data)
            
            # example fields
            # author = row["author"]
            # subreddit = row["subreddit"]
            # id = row["id"]
            # created = row["created_utc"]
            # score = row["score"]
            # posts only
            # title = row["title"]
            # body = row["selftext"]
            # url = row["url"]
            # comments only
            # body = row["body"]
            # parent = row["parent_id"]    # id/name of the parent comment or post (e.g. t3_abc123 or t1_abc123)
            # link_id = row["link_id"]    # id/name of the post (e.g. t3_abc123)
        progressLog.logProgress("\n")
        
        with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(matched_posts, f, indent=4, ensure_ascii=False)
               
        print(f"Filtered posts written to {OUTPUT_JSON_PATH}")

def processFolder(path: str):
    fileIterator: Iterable[str]
    if recursive:
        def recursiveFileIterator():
            for root, dirs, files in os.walk(path):
                for file in files:
                    yield os.path.join(root, file)
        fileIterator = recursiveFileIterator()
    else:
        fileIterator = os.listdir(path)
        fileIterator = (os.path.join(path, file) for file in fileIterator)
    
    for i, file in enumerate(fileIterator):
        print(f"Processing file {i+1: 3} {file}")
        processFile(file)

def main():
    if os.path.isdir(fileOrFolderPath):
        processFolder(fileOrFolderPath)
    else:
        processFile(fileOrFolderPath)
    
    print("Done :>")

if __name__ == "__main__":
    main()

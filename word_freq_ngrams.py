import os
import sys
import gzip
import time
import subprocess
from collections import defaultdict
from urllib.parse import urlparse
from urllib.request import urlretrieve

from tqdm.notebook import tqdm


################################################
############ INPUT AND OUTPUT FILES ############
################################################

files = (
    "http://storage.googleapis.com/books/ngrams/books/20200217/eng/1-00000-of-00024.gz",
    # ... more gz files
    # English 1-grams files: 
    # https://storage.googleapis.com/books/ngrams/books/20200217/eng/eng-1-ngrams_exports.html
)

word_freq_file = "eng_freq.tsv" # raw filtered data
word_pos_freq_file = "eng_word_pos_freq.tsv" # posprocessed data

################################################
####### DOWNLOAD RAW DATA AND PREPROCESS #######
################################################

def parse_line(line):
    # Line format:
    # word  y1,match1,vol1  y2,match2,vol2  ... (whitespace-separated)
    parts = line.strip().split()
    if not parts:
        return None

    word = parts[0] # keep as is, e.g. "1,387,805_NUM"
    total = 0

    for triple in parts[1:]:
        try:
            year, match_count, volume_count = triple.split(",", 2)
            total += int(match_count)
        except ValueError:
            continue

    return word, total


def _download(url, dst_path):
    # Try wget first
    try:
        subprocess.run(
            ["wget", "-q", "-O", dst_path, url],
            check=True
        ) # fallback to Python download if wget is unavailable.
    except (FileNotFoundError, subprocess.CalledProcessError):
        urlretrieve(url, dst_path)

def process_gz_file(gz_url_or_path, out_path, tmp_dir="."):
    """
    - If gz_url_or_path is an HTTP(S) URL: download to tmp_dir.
    - Stream the gzip, sum counts per word across years (within this file).
    - Append "word<TAB>count\n" for each word to out_path.
    - Delete the downloaded gz when done.
    """
    is_url = gz_url_or_path.startswith(("http://", "https://"))
    if is_url:
        fname = os.path.basename(urlparse(gz_url_or_path).path)
        if not fname:
            raise ValueError("Cannot infer filename from URL.")
        gz_path = os.path.join(tmp_dir, fname)
        _download(gz_url_or_path, gz_path)
        cleanup = True
    else:
        gz_path = gz_url_or_path
        cleanup = False

    counts = defaultdict(int)

    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            parsed = parse_line(line)
            if not parsed:
                continue
            w, c = parsed
            counts[w] += c

    # Append results for this file
    with open(out_path, "a", encoding="utf-8") as out:
        for w, c in counts.items():
            out.write(f"{w}\t{c}\n")

    if cleanup:
        try:
            os.remove(gz_path)
        except OSError:
            pass

for file in files:
    print("Processing file:", file)
    start = time.time()
    process_gz_file(file, "eng_freq.tsv") # output file
    end = time.time()
    print(f"Time elapsed: {end - start:.2f}s")

################################################
######### POSTPROCESS FREQ COUNT DATA ##########
################################################

def split_word_pos(token: str):
    if token.startswith("_") and token.endswith("_"):
        return token, token
    if "_" in token:
        base, pos = token.rsplit("_", 1)
        return base, pos
    return token, "_"  # placeholder

def convert_and_sort(in_path, out_path):
    print("Parsing data")
    rows = []
    with open(in_path, "r", encoding="utf-8") as fin:
        for line in tqdm(fin):
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                word, count_str = line.split("\t", 1)
                count = int(count_str)
            except ValueError:
                continue
            base, pos = split_word_pos(word)
            rows.append((count, base, pos))
    
    print("Sorting...")
    rows.sort(key=lambda x: x[0], reverse=True)
    
    print("Writing to file...")
    with open(out_path, "w", encoding="utf-8") as fout:
        for count, base, pos in tqdm(rows):
            fout.write(f"{base}\t{pos}\t{count}\n")

convert_and_sort(word_freq_file, word_pos_freq_file)

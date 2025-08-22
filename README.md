# googlebook_ngrams
Google Books Ngram Processor:

1. Download raw data from Google Books Ngram
2. Extract word and counts (sums up across all years)
3. Deletes gz file
4. Filters POS tag to new column if POS tag available, otherwise it adds `_`.
5. Resulting file is a 3-column tsv file `word\tPOS\tfreq` sorted by descending counts.

**Adjust variables in `word_freq_ngrams.py`**

`files`: gz files to download and process.

These can be downloaded from Google Books Ngrams (e.g. https://storage.googleapis.com/books/ngrams/books/20200217/eng/eng-1-ngrams_exports.html)

`word_freq_file`:  Stores raw filtered data (step 2)

`word_pos_freq_file`: Stores resulting file (step 5)

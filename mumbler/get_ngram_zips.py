import os
import sys
import requests

BASE_NGRAM_URL="http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-us-all-2gram-20090715-{}.csv.zip"
DATA_DIR="./data"

def get_range(start_index, end_index):

    for index in range(start_index, end_index+1):
        ngram_url = BASE_NGRAM_URL.format(index)
        print("Filename: ", get_filename(ngram_url))

        download_file(ngram_url)

    print("...done.")


def get_filename(url):
    filename = ""

    if url.find('/'):
        filename=url.rsplit('/',1)[1]

    return filename

def download_file(url):
    filename = get_filename(url)
    full_path = DATA_DIR+"/"+filename
    if not os.path.exists(full_path):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        response = requests.get(url, allow_redirects=True, stream=True)

        with open(full_path, 'wb') as file_handle:
            for piece in response.iter_content(1024):
                if piece:
                    file_handle.write(piece)




if __name__ == "__main__":

    print("Command line: ", str(sys.argv))
    if len(sys.argv) != 3:
        print("\nUsage: python get_ngram_zips <start_index> <end_index>")
    else:
        if not os.path.exists(os.path.dirname(DATA_DIR)):
            os.makedirs(DATA_DIR)
        get_range(int(sys.argv[1]), int(sys.argv[2]))
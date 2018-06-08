#!/usr/local/bin/python2.7
from configparser import ConfigParser
import glob
import io
from io import TextIOWrapper
import json
import os
from os.path import basename
import re
import shutil
import sys
import time
import zipfile

import requests

DEV = True
DATA_DIR = "./data"
JSON_DIR = DATA_DIR+"/json"
PARSE_CSV_FILE_NAME_PATTERN = DATA_DIR+"/{}.{}.parse.csv"
IGNORED_LINES_PATTERN = DATA_DIR+"/ignored.lines.{}.csv"
FILE_MINIMUM_AGE = 60
MAX_ITERS = 3

save_ignored_lines = False
compress_result_files = False

# The following regex template matches a single starting character, specified per node, followed by any characters that would match for any node,
# then optionally matches a space and any set of characters that would could be matched by any node, followed by year, match_count, page_count and
# volume_count
#NGRAM_REGEX_FORMAT = "(?P<word1>{}+[a-zA-Z]*)(?P<word2>[ ][a-zA-Z]*)?\t(?P<year>d+)\t(?P<match_count>d+)\t(?P<page_count>d+)\t(?P<volume_count>d+)"
NGRAM_REGEX_FORMAT = "(?P<word1>[a-zA-Z\'\"\.]+[a-zA-Z\'\"\.]*) ?(?P<word2>[a-zA-Z'\"\.]*)?\t(?P<year>\d+)\t(?P<match_count>\d+)\t(?P<page_count>\d+)\t(?P<volume_count>\d+)"





def read_config():

    global config
    global letters_words_counts
    global node_regex
    global other_nodes_regex
    global ignored_lines_filename

    config = ConfigParser()
    letters_words_counts = {}
    other_nodes_regex = {}

    if os.path.isfile("mumbler.parse.cfg"):
        try:
            config.read("mumbler.parse.cfg")
        except Exception as e:
            print("Couldn't read config file: "+str(e))
    else:
        print "mumbler.parse.cfg not found!"
        sys.exit(-1)

    try:
        node_id = config.get("node","node_id")
        node_regex_conf = str(config.get("patterns", node_id+".regex"))
        all_nodes = config.get("node", "all_nodes")
        for some_node_id in all_nodes.split(","):
            if not (node_id == some_node_id):
                some_regex_conf = str(config.get("patterns", some_node_id+".regex"))
                other_nodes_regex[str(some_node_id)] = some_regex_conf

    except Exception as e:
        print("Couldn't read config section and variable: "+str(e))

    print "node_regex_conf: "+ node_regex_conf
    node_regex = re.compile(node_regex_conf)

def age_greater_than(file_path, age, start_time):

    stat = os.stat(file_path)
    if (stat.st_mtime - start_time > age) | \
        (stat.st_ctime - start_time > age) :
        return False
    else:
        return True


def collect_word_trees(node_id, node_dir, trees_dir):

    COMPONENT_JSON_FILE_PATTERN = r"[a-z]_tree."+node_id+"\.\d+\.\d+\.json"
    component_file_pattern = re.compile(COMPONENT_JSON_FILE_PATTERN)
    forest = {}

    # counter of files collected into the forest
    fragment_count = 0

    for root, dirs, files in os.walk(node_dir):

        for json_file in files:

            # select only the files matching <letter>_gpfs?.<time_seconds>
            if (node_regex.match(json_file[0]) is not None) & (component_file_pattern.match(json_file) is not None):

                    letter_files = forest.get(json_file[0])
                    if not letter_files:
                        letter_files = []
                        forest[json_file[0]] = letter_files

                    letter_files.append(json_file)

    for first_letter in forest.iterkeys():
        collected_words = {}

        letter_files = forest.get(first_letter)
        for json_file in letter_files:

            fragment_count += 1

            file_path = node_dir + "/" + json_file

            with open(file_path, "r") as json_fp:
                letter_tree = json.load(json_fp)

                for new_word, new_word_entry in letter_tree.iteritems():

                    # This word may already be in the collection

                    if collected_words.has_key(new_word):
                        existing_word = collected_words.get(new_word)
                        existing_word["count"] = existing_word.get("count") + new_word_entry.get("count")

                        existing_next_words = existing_word.get("next")
                        next_words = new_word_entry.get("next")

                        if next_words:
                            for another_word, another_entry in next_words.iteritems():
                                if another_word:
                                    if another_word in existing_next_words.iterkeys():
                                        existing_next_entry = existing_next_words.get(another_word)
                                        existing_next_entry["count"] = existing_next_entry.get("count") + another_entry.get("count")
                                    else:
                                        existing_next_words[another_word] = another_entry
                    else:
                        collected_words[new_word] = letter_tree.get(new_word)

            os.remove(file_path)

        letter_words_filename = first_letter + "_tree.json"
        letter_words_filepath = node_dir + "/" + letter_words_filename
        letter_words_zippath = letter_words_filepath + ".zip"

        if os.path.isfile(letter_words_filepath):
            print "Skipping file \"" + letter_words_filepath + "\" due to existing file."
        else:
            with open(letter_words_filepath, "w") as json_file:
                json.dump(collected_words, json_file)

            if os.path.isfile(letter_words_zippath):
                mode = "a"
            else:
                mode = "w"

            with zipfile.ZipFile(letter_words_zippath, mode) as zip_file:

                if not letter_words_filename in zip_file.filelist:
                    zip_file.write(letter_words_filepath, arcname=letter_words_filepath)

            os.remove(letter_words_filepath)

    return fragment_count, len(forest)



def move_matched_files(node_id, node_dir, node_regex):

    # Each file will be compared for most recent modification times
    # Files must have had no modifications for a configured interval prior to this processing
    # and they will be ignored in this loop if they have been modified too recently.

    start_time = time.time()
    age = FILE_MINIMUM_AGE


    for root, dirs, files in os.walk(JSON_DIR):
        print "root="+ root
        print "dirs=" + str(dirs)
        print "files=" + str(files)

        # capture the count of files not ready to be moved
        remaining_files = 0

        # Iterate through the JSON files, for each one checking if the first letter matches the regex for this node
        for json_file in files:
            if node_regex.match(json_file[0]):

                file_path = root + "/" + json_file
                if age_greater_than(file_path, age, start_time):
                    if (DEV):
                        shutil.copy(file_path, node_dir)
                    else:
                        shutil.move(file_path, node_dir)
                else:
                    remaining_files += 1

        return remaining_files

    sys.exit(0)


if __name__ == "__main__":

    global config
    global letters_words_counts
    global ignored_lines_zip_files
    global other_nodes_zip_files
    global other_nodes_csv_files

    ignored_lines_zip_files = {}
    other_nodes_zip_files = {}
    other_nodes_csv_files = {}

    read_config()

    node_id = config.get("node", "node_id")
    trees_dir = JSON_DIR + "/" + node_id

    iterations = MAX_ITERS

    node_dir = DATA_DIR + "/" + node_id

    while (move_matched_files(node_id, node_dir, node_regex) > 0) & (iterations > 0):
        time.sleep(FILE_MINIMUM_AGE/2)
        iterations -=1

    fragment_count, aggregate_count = collect_word_trees(node_id, node_dir, trees_dir)

    print("postprocess_ngram_zip: processed {} JSON files, aggregating into {} JSON word counts trees".format(fragment_count, aggregate_count))


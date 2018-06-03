import codecs
from configparser import ConfigParser
import glob
import json
import os
import re
import sys
import time
import zipfile

import requests

DATA_DIR = "./data"
JSON_DIR = DATA_DIR+"/json"
PARSE_CSV_FILE_NAME_PATTERN = DATA_DIR+"/{}.{}.parse.csv"
IGNORED_LINES_PATTERN = DATA_DIR+"/ignored.lines.{}.csv"

# The following regex template matches a single starting character, specified per node, followed by any characters that would match for any node,
# then optionally matches a space and any set of characters that would could be matched by any node, followed by year, match_count, page_count and
# volume_count
NGRAM_REGEX = "(?P<word1>{}+[a-zA-Z.'\"]*)(?P<word2>\ [a-zA-Z.'\"]*)?\t(?P<year>\d+)\t(?P<match_count>\d+)\t(?P<page_count>\d+)\t(?P<volume_count>\d+)"

#node_regex = None
#config = configparser.RawConfigParser()
#letters_words_counts = {}

def process_zip_file(zip_file_path, letters_words_counts):
    global node_regex
    global ignored_lines_file

    with zipfile.ZipFile(zip_file_path, "r") as zip_file:
        for csv_filename in zip_file.namelist():
            with zip_file.open(csv_filename) as csv_file:
                for line in csv_file:
                    line = str(codecs.encode(line, "utf8"))

                    reg_match = node_regex.match(line)
                    if reg_match:
                        word1 = reg_match.group('word1')
                        word2 = reg_match.group('word2')
                        year = reg_match.group('year')
                        match_count = reg_match.group('match_count')
                        page_count = reg_match.group('page_count')
                        volume_count = reg_match.group('volume_count')
                        # print("ngram: \"{}\", year: \"{}\", match_count: \"{}\", page_count: \"{}\", volume_count: \"{}\"".format(ngram, year, match_count, page_count, volume_count))

                        process_ngram(word1, word2, int(match_count), letters_words_counts)
                    else:
                        write_other_node_file(line)

def close_files():

    PARSE_ZIP_FILE_NAME_PATTERN = DATA_DIR+"/{}.{}.parse.csv.zip"

    global ignored_lines_file
    global other_nodes_csv_files

    if ignored_lines_file:
        ignored_lines_file.flush()
        ignored_lines_file.close()

    if other_nodes_csv_files:
        for csv_file in other_nodes_csv_files.itervalues():
            csv_file.flush()
            csv_file.close()

    # iterate over all the parse files for other nodes, put them into zips and delete the original files
    for path in glob.glob(DATA_DIR+"/gpfs?."+node_id+".parse.csv"):
        zip_file_path = path + ".zip"

        specific_path = path + "." + str(time.time())
        os.rename(path, specific_path)

        with zipfile.ZipFile(zip_file_path, "a") as zip_file:
            zip_file.write(specific_path)

        os.remove(specific_path)

    # iterate over all the JSON letter files, put them into zips and delete the original files
    zip_file_path = str(JSON_DIR+"/letter_trees." + node_id + "." + str(time.time()) + ".zip")
    for path in glob.glob(JSON_DIR+"/*_tree."+node_id+".*.json"):

        with zipfile.ZipFile(zip_file_path, "a") as zip_file:
            zip_file.write(path)

        os.remove(path)

    # zip the ignored lines file and delete the original
    ignored_lines_filename = IGNORED_LINES_PATTERN.format(node_id)

    if os.path.exists(ignored_lines_filename):
        ignored_zip = ignored_lines_filename + ".zip"

        # rename the ignored lines file, so that it can be collected with others for this node in a zip
        ignored_specific_filename = ignored_lines_filename + "." + str(time.time())
        os.rename(ignored_lines_filename, ignored_specific_filename)

        with zipfile.ZipFile(ignored_zip, "a") as zip_file:
            zip_file.write(ignored_specific_filename)

        os.remove(ignored_specific_filename)

def write_other_node_file(line):

    global ignored_lines_file
    global other_nodes_csv_files
    global other_nodes_zip_files

    matched = False
    ignored_lines_filename = IGNORED_LINES_PATTERN.format(node_id)

    for some_node_id, some_regex in other_nodes_regex.iteritems():
        other_regex = re.compile(NGRAM_REGEX.format(some_regex, some_regex))
        if other_regex.match(line):
            matched = True

            other_node_csv_file_path = PARSE_CSV_FILE_NAME_PATTERN.format(some_node_id, node_id)
            other_node_csv_file = other_nodes_csv_files.get(some_node_id)

            if not other_node_csv_file:
                other_node_csv_file = open(other_node_csv_file_path, "a+")
                other_nodes_csv_files[some_node_id] = other_node_csv_file

            other_node_csv_file.write(line)

    if not matched:
        if not ignored_lines_file:
            ignored_lines_file = codecs.open(IGNORED_LINES_PATTERN.format(node_id), "a+", encoding="utf8")
        ignored_lines_file.write(line)

def process_ngram(first_word, second_word, match_count, letters_words_counts):

    root_words = letters_words_counts["next"]
    if not root_words:
        root_words = {}
        letters_words_counts["next"] = root_words

    first_word = first_word.lower()
    if second_word:
        second_word = second_word.lower()

    #one_grams = ngram.split()
    #first_word = one_grams[0].lower()
    #second_word = None
    #if (len(one_grams) > 1):
        #second_word = one_grams[1].lower()

    this_word_counts = root_words.get(first_word)

    if not this_word_counts:
        this_word_counts = {}
        this_word_counts["count"] = match_count
        root_words[first_word] = this_word_counts
    else:
        new_count = this_word_counts["count"] + match_count
        this_word_counts["count"] = new_count

    letters_words_counts["count"] += match_count

    if second_word:
        second_words_set = this_word_counts.get("next")

        if not second_words_set:
            second_words_set = {}
            this_word_counts["next"] = second_words_set

        second_word_counts = second_words_set.get(second_word)

        if not second_word_counts:
            second_word_counts = {}
            second_word_counts["count"] = match_count
            second_words_set[second_word] = second_word_counts
        else:
            second_word_counts["count"] = second_word_counts["count"] + match_count


def split_by_letter(letter_words_counts):

    letter_trees = {}

    for word, counts in letter_words_counts.iteritems():

        letter_key = word[0]

        if not re.match("[a-zA-Z]", letter_key):
            letter_key = "punct"

        letter_dict = letter_trees.get(letter_key)

        if not letter_dict:
            letter_dict = {}
            letter_trees[letter_key] = letter_dict

        letter_dict[word] = counts

    for letter, tree in letter_trees.iteritems():
        filepath = JSON_DIR+"/"+letter+"_tree."+node_id+"."+str(time.time())+".json"
        with codecs.open(filepath, "w", "utf8") as dict_file:
            dict_file.write(json.dumps(tree))


def read_config():

    global config
    global letters_words_counts
    global node_regex
    global other_nodes_regex
    global ignored_lines_filenamea

    config = ConfigParser()
    letters_words_counts = {}
    other_nodes_regex = {}

    if os.path.isfile("mumbler.parse.cfg"):
        try:
            config.read("mumbler.parse.cfg",encoding="utf8")
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
    node_regex = re.compile(NGRAM_REGEX.format(node_regex_conf))

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

    json_file_name = JSON_DIR+"/letters_words_counts."+node_id+".json"

    # Check if the node-specific dictionary JSON file exists and open for append if so
    if os.path.exists(json_file_name):
        with open(json_file_name, "r") as json_file:
            letters_words_counts = json.load(json_file)

    # Otherwise, just create an empty dict to start
    else:
        letters_words_counts = {}

    letters_words_counts["count"]=0
    letters_words_counts["next"]={}

    # Initialize the ignored file to None, to support creation/appending logic
    # And, get the filename to one specific to this node
    ignored_lines_file = None
    ignored_lines_filename = IGNORED_LINES_PATTERN.format(node_id)


    print("Command line: ", str(sys.argv))
    if len(sys.argv) != 2:
        print("\nUsage: python process_ngram_zip <zip file path>")
    else:
        process_zip_file(sys.argv[1], letters_words_counts)

        split_by_letter(letters_words_counts)

        close_files()



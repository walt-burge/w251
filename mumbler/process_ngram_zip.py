import codecs
from configparser import ConfigParser
import json
import os
import re
import sys
import zipfile

import requests

DATA_DIR = "./data"
PARSE_FILE_NAME_PATTERN = DATA_DIR+"/{}.{}.parse.csv"
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

    with zipfile.ZipFile(zip_file_path) as zip_file:
        for csv_filename in zip_file.namelist():
            with zip_file.open(csv_filename) as csv_file:
                for line in csv_file:
                    line = str(line)

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

    ignored_lines_file.flush()
    ignored_lines_file.close()


def write_other_node_file(line):

    global ignored_lines_file

    matched = False
    PARSE_FILE_NAME_PATTERN = DATA_DIR+"/{}.{}.parse.csv"
    IGNORED_LINES_PATTERN = DATA_DIR+"/ignored.lines.{}.csv"
    ignored_lines_filename = IGNORED_LINES_PATTERN.format(node_id)

    for some_node_id, some_regex in other_nodes_regex.iteritems():
        other_regex = re.compile(NGRAM_REGEX.format(some_regex, some_regex))
        if other_regex.match(line):
            matched = True
            with codecs.open(PARSE_FILE_NAME_PATTERN.format(some_node_id, node_id), "a+",encoding="utf8") as other_file:
                other_file.write(line)

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


def read_config():

    global config
    global letters_words_counts
    global node_regex
    global other_nodes_regex
    global ignored_lines_filename
    global ignored_lines_file

    global ignored_lines_file


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

    read_config()

    node_id = config.get("node", "node_id")

    json_file_name = DATA_DIR+"/letters_words_counts."+node_id+".json"

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
        with codecs.open(DATA_DIR + "/letters_words_counts."+node_id+".json", "w", "utf8") as dict_file:
            dict_file.write(json.dumps(letters_words_counts))

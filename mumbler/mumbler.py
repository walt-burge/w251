#!/usr/local/bin/python2.7
import base64
from configparser import ConfigParser
import io
import json
import numpy as np
import numpy.random as rand
import os
import re
import sys
import zipfile

import requests

DATA_DIR = "./data"

# The following regex template matches a single starting character, specified per node, followed by any characters that would match for any node,
# then optionally matches a space and any set of characters that would could be matched by any node, followed by year, match_count, page_count and
# volume_count
NGRAM_REGEX = "(?P<word1>{}+[a-zA-Z\.\'\"]*)(?P<word2> [a-zA-Z\.\'\"]*)?\t(?P<year>d+)\t(?P<match_count>d+)\t(?P<page_count>d+)\t(?P<volume_count>d+)"

def read_config():

    global config
    global words_tree_folder
    global node_regex
    global nodes_regex

    config = ConfigParser()
    node_regex = None
    nodes_regex = {}

    if os.path.isfile("mumbler.parse.cfg"):
        try:
            config.read("mumbler.parse.cfg")
        except Exception as e:
            print("Couldn't read config file: "+str(e))
            sys.exit(-1)
    else:
        print "mumbler.parse.cfg not found!"
        sys.exit(-1)


    try:
        node_id = str(config.get("node","node_id"))
        all_nodes = str(config.get("node", "all_nodes"))
        for some_node_id in all_nodes.split(","):

            some_regex_conf = str(config.get("patterns", some_node_id+".regex"))
            if (node_id == some_node_id):
                node_regex_conf = some_regex_conf
            else:
                nodes_regex[str(some_node_id)] = some_regex_conf

    except Exception as e:
        print("Couldn't read config section and variable: "+str(e))
        sys.exit(-1)

    node_regex = re.compile(node_regex_conf)

    words_tree_folder = DATA_DIR + "/" + node_id


class response_msg():
    def __init__(self):
        self.word = None

    def to_string(self):
        return "<>"

    def render(self):
        if word:
            return "<response_msg:"+encode_word(self.word)+">"
        else:
            return "<response_msg:>"


class empty_msg(response_msg):
    def __init__(self, word):
        self.word = word

    def to_string(self):
        str_val = "<empty"

        if (self.word):
            str_val += ":"
            str_val += base64.b64encode(self.word)

        str_val += ">"

        return str_val

    def render(self):
        return self.to_string()


class forward_msg(response_msg):
    def __init__(self, word, node, num_words):
        self.word = word
        self.node = node
        self.num_words = num_words

    def to_string(self):
        str_val = "<forward:"

        if self.word:
            str_val += base64.b64encode(self.word)
        str_val += ":"
        str_val += self.node
        str_val += ":"
        str_val += str(self.num_words)
        str_val += ">"

        return str_val

    def render(self):
        return self.to_string()



def add_ext_call(word, num_words):

    ext_call = empty_msg(None)

    if word:
        if num_words:
            for some_node_id, some_node_regex in nodes_regex.iteritems():
                if re.match(some_node_regex, word[0]):
                    ext_call = forward_msg(word, some_node_id, num_words)
        else:
            ext_call = empty_msg(word)
    else:
        ext_call = empty_msg(None)

    return ext_call


def get_words_tree(word):

    global words_tree_folder

    words_tree = None

    if re.match(node_regex, word):
        first_letter = word[0]

        if not re.match("[a-zA-Z]", first_letter):
            first_letter = "punct"

        words_tree_json_path = words_tree_folder + "/" + first_letter + "_tree.json"
        words_tree_zip_path = words_tree_json_path + ".zip"

        if (os.path.exists(words_tree_zip_path)):
            with zipfile.ZipFile(words_tree_zip_path, "r", allowZip64=True) as zip_file:
                for json_filename in zip_file.namelist():
                    with zip_file.open(json_filename) as words_tree_file:
                        words_tree = json.load(words_tree_file)

    return words_tree


def get_words(current_word, num_words, total_count):

    global words_tree_folder

    words = []

    while (len(words)+1) < num_words:

        if node_regex.match(current_word):

            words_tree = get_words_tree(current_word)

            if (words_tree):
                current_word_counts = words_tree.get(current_word)

                if current_word_counts:
                    word_counts = current_word_counts["next"]
                    total_count = current_word_counts["count"]
                else:
                    word_counts = None

                if word_counts:
                    next_word = get_weighted_choice(word_counts, total_count)
                    if next_word:
                        words.append(next_word)
                        current_word = next_word
                    else:
                        words.append(add_ext_call(None, None))
                        break
                else:
                    words.append(add_ext_call(None, None))
                    break
        else:
            words.append(add_ext_call(current_word, num_words-len(words)))
            break

    return words


def get_weighted_choice(next_words, total_count):
    next_words_total_sum = 0

    probs = []
    words = list(next_words.keys())
    for word in words:
        word_count = next_words.get(word)
        this_count = word_count["count"]
        next_words_total_sum += this_count
        probs.append(round(float(this_count)/float(total_count),16))

    # To account for numpy.random.choice tolerance/rounding issue,
    # divide all the probabilities by their total sum, thus normalizing
    probs = np.array(probs)
    probs /= probs.sum()

    choice_word = rand.choice(a=words,p=probs)
    return choice_word


def usage():
    print("\nUsage: python mumbler <starting_word> <max # of words>\n")

def encode_word(word):
    encoded_word = "<" + base64.b64encode(word) + ">"
    return encoded_word


if __name__ == "__main__":

    read_config()

    global node_regex

    first_word = sys.argv[1]

    if (len(sys.argv) != 3) | (not sys.argv[1]) | (len(sys.argv[1]) == 0):
        usage();
        sys.exit(-1)
    else:
        max_words = 0

        try:
            max_words = int(sys.argv[2])
        except Exception as e:
            usage();
            sys.exit(-1)

        if (len(first_word) > 0):
            if node_regex.match(first_word[0]):

                words = get_words(first_word, max_words-1, 100000000000)
                print(encode_word(first_word))
                for word in words:
                    if isinstance(word, response_msg):
                        print word.render()
                    else:
                        print(encode_word(word))
            else:
                ext_call = add_ext_call(first_word, max_words)



#!/usr/local/bin/python2.7
import base64
from configparser import ConfigParser
import io
import json
import numpy as np
import numpy.random as rand
import os
import re
import subprocess
import sys
import zipfile

import requests

DATA_DIR = "./data"

# The following regex template matches a single starting character, specified per node, followed by any characters that would match for any node,
# then optionally matches a space and any set of characters that would could be matched by any node, followed by year, match_count, page_count and
# volume_count
EMPTY_REGEX = "<empty(?P<word2>:[a-zA-Z.'\"]*)?"
FORWARD_REGEX = "<forward:(?P<encoded_word>.*):(?P<node_id>.[a-zA-Z0-9]*):(?P<max_words>[0-9]*)>"
WORD_REGEX = "(?P<word>[a-zA-Z.'\"]*)"
ENCODED_WORD_REGEX = "<(?<encoded_word>[.]*)>"


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

            nodes_regex[str(some_node_id)] = some_regex_conf

    except Exception as e:
        print("Couldn't read config section and variable: "+str(e))

    node_regex = re.compile(node_regex_conf)

    words_tree_folder = DATA_DIR + "/" + node_id


class response_msg():
    def __init__(self):
        self.word = None

    def to_string(self):
        return "<>"


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

        words_tree_path = words_tree_folder + "/" + first_letter + "_tree.json"
        if os.path.exists(words_tree_path):

            with io.open(words_tree_path, "r") as words_tree_file:
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

    response_items = []

    for item in words:
        if isinstance(item, response_msg):
            response_items.append(item.encode())
        else:
            response_items.append(item)

    return response_items


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


def get_forward_node(word):

    forward_node = None

    for some_node_id, some_node_regex in nodes_regex.iteritems():
        if re.match(some_node_regex, word[0]):
            forward_node = some_node_id

    return forward_node


def ssh_call(forward_node_id, command):

    result = subprocess.check_output(["ssh", "root@"+forward_node_id, command])

    return result


def forward_call(forward_node_id, first_word, max_words):
    result = ssh_call(forward_node_id, "cd /opt/w251/mumbler; python2.7 /opt/w251/mumbler/mumbler.py " + first_word + " " + str(max_words))

    return result


if __name__ == "__main__":

    read_config()

    empty_regex = re.compile(EMPTY_REGEX)
    forward_regex = re.compile(FORWARD_REGEX)
    word_regex = re.compile(WORD_REGEX)

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

        if len(first_word) > 0:


            forward_node_id = get_forward_node(first_word)

            word = None
            word_count = 0

            sys.stdout.write(first_word)
            sys.stdout.flush()
            word_count += 1

            while word_count < max_words:

                forward_call_response = forward_call(forward_node_id, first_word, max_words)
                print "Forward call response: ",forward_call_response
                for word in forward_call_response.split("\n"):

                    if word:

                        #encoded_match = re.match(word, ENCODED_WORD_REGEX)

                        #if encoded_match:
                        #    encoded_word = encoded_match.group("encoded_word")
                        #    word = base64.b64decode(encoded_word)

                        word_match = re.match(word, ENCODED_WORD_REGEX)
                        forward_match = forward_regex.match(word)
                        empty_match = empty_regex.match(word)
                        if forward_match:
                            encoded_word = forward_match.group("encoded_word")
                            decoded_word = base64.b64decode(encoded_word)
                            forward_node_id = forward_match.group("node_id")
                            max_words = forward_regex.match("max_words")
                            break
                        else:

                            if word_match:
                                encoded_word = word_match.group("encoded_word")
                                word = base64.b64decode(encoded_word)
                                sys.stdout.write(" "+word)
                                sys.stdout.flush()
                                word_count += 1
                            else:
                                max_words = 0
                                break
                    else:
                        max_words = 0
                        break







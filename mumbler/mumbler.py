import json
import numpy as np
import numpy.random as rand
import os
import re
import sys
import zipfile

import requests

DATA_DIR = "./data"
LETTERS_WORDS_JSON = DATA_DIR + "/letters_words_counts"

letters_words_counts = {}

def get_words(current_word, num_words, next_words, total_count):
    words = []

    while (len(words)+1) < num_words:

        if current_word:
            current_word_counts = next_words.get(current_word)
            if current_word_counts:
                word_counts = current_word_counts["next"]
            else:
                word_counts = None
        else:
            word_counts = next_words

        if word_counts:
            next_word = get_weighted_choice(word_counts, total_count)
            if next_word:
                words.append(next_word)
                current_word = next_word
            else:
                break
        else:
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

if __name__ == "__main__":

    with open(LETTERS_WORDS_JSON, "r") as json_file:
        letters_words_counts = json.load(json_file)

    print("Command line: ", str(sys.argv))
    if len(sys.argv) != 3:
        print("\nUsage: python mumbler <starting_word> <max # of words>\n")
    else:
        words = get_words(sys.argv[1], int(sys.argv[2]),
                          letters_words_counts.get("next"), letters_words_counts.get("count"))
        print("Initial Word: \"{}\"".format(sys.argv[1]))
        for word in words:
            print("Next Word: \"{}\"".format(word))


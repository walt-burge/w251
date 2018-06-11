# Homework 4 : GPFS + Mumbler

## Overview

The mumbler project is located under /root/mumbler on nodes gpfs1, gpfs2 and gpfs3. Under this, there is a "data" subfolder that is a soft-link to /gpfs/gpfsfpo/data, which is the GPFS filesystem created in the context of the GPFS cluster with nodes gpfs1 (quorum), gpfs2, and gpfs3.

###Data Acquisition/Preproprocessing

|  File | Functionality |
|:-------------------------------------------------|:-------------|
| ***/root/mumbler/get\_ngram\_zips.py*** | download a range of google ngram zip files to the mumbler/data folder |
| ***/root/mumbler/process\_ngram\_zip.py*** | preprocess a given google ngram zip, creating separate JSON files <letter>+ "_tree." + node_id + "." + unix timestamp + ".json" each with initial words begining with the same variable in a given google ngram zip |
| ***/root/mumbler/postprocess_zip.py*** | given the regex configuration (mumbler.parse.cfg) for the current node, copy all JSON files with a first letter included matching the configured node regex to mumbler/data/<node_id> and collect together word counts to produce a single <letter>_... file for each letter included in regex for node.
|

###Mumbler App

File **/root/mumbler/mumbler_app.py** is the one to be executed in the following manner at the command line:

1. ***cd /root/mumbler***
2. ***python2.7 mumbler_app.py <first word> <max words>***

While this part of the functionality has not been completed, this script will examine the first letter of the specified ***<first word>*** and determine from the ***mumbler.parse.cfg*** which node should handle the request, gpfs1, gpfs2 or gpfs3. In the ***forward_call*** method, (in turn calling ***ssh_call***), the ***subprocess*** package will be used for issuing a ***subprocess.check_output*** to ssh to the indicated node and issue the following command sequence:

***cd /opt/w251/mumbler; 
python2.7 /opt/w251/mumbler/mumbler.py " + first_word + " " + str(max_words)***

This will delegate the words lookup to the node configured to handle the words starting with the same first letter as the word specified on the command line.

The responses will be a list of lines with the following contents (each enclo:

***"\<" + b64-encrypted_word + "\>"***
***"\<forward:" + b64-encrypted word + ":" + ":" + forward node_id + ":" + remaining max words + ">"***
***"\<empty" [ + ":" + b64-encrypted word ]  + "\>"***


###Mumbler - worker (***mumbler.py***)

1. In the first of the above response type examples, the ***\<b64-encrypted word\>*** sequenced by line starts with the specified word and continues on each subsequent line with words in the resulting chain. The words are b64-encrypted as an initial precaution to simplify response syntax and the possibility of some words that might contain characters that would complicate parsing. That did not end up being the case.
2. In the second of these line types, (***\<forward...***), it is determined that the next word to be looked up is handled by a different node, and includes the ***node_id*** and ***remaining max_words*** that should be sent along with the ***b64-encrypted word***.
3. In the third case, the ***\<empty...*** line type represents the fact that a next word has not been found in the JSON trees.	

The words are looked up in the appropriate ***mumbler/data/\<node_id\>/\<letter\>\_tree.json.zip*** file, which contains the collected word counts for letters having the ***\<letter\>*** indicated in the filename.

These JSON files are structured as follows:

***{"and" : {"count" : 8, "next" : {"tomorrow" : {"count" : "8"} } } }             
}***

In each case of a word with a sub-dict containing only a ***"count"*** element, there is no navigable next word. In cases in which the sub-dict contains also a ***"next"*** element, this will provide a sub-dict with a list of navigable words. The preprocessing functionality required collecting and weaving together these counts so that counts at a lower level were aded to counts at a higher level.


###Bad news

While this functionality works with a single node (i.e. my laptop), and a zip file containing the data specified in the homework README (Homework - part 2 - The Mumbler), the preprocessing step along that I'm running on these three nodes is currently running without returning for several days.

But, the preprocessing does enable the randomization of subsequent words using the next word frequency as compared to the current word frequency. And the ***mumbler.py*** script properly b64-encodes the words and properly sends the ***\<forward...*** and ***\<empty...*** responses as described.
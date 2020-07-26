__author__ = 'Harsh'
from pyspark import SparkContext
from operator import add
import re


def splitter_word(sentence):
    #change all words to lowercase, splitting sentence to words
    sentence=sentence.lower()
    words=sentence.split()
    return words

def splitter_phrase(sentence):
    words=sentence.split()
    return (a+' '+b for a,b in zip(words,words[1:]))


sc= SparkContext('local','wordcount')
text=sc.textFile('../bin/myfiles/twitter_texts.txt')

words=text.flatMap(splitter_word)   # splitting of each sentence into words

words_mapped =words.map(lambda x: (x,1))   # creatig tuples with key as word and its value as 1 (as an initial count)

counts_w =words_mapped.reduceByKey(add)      # adding all the counts and aggregating same words
filter_w=counts_w.collectAsMap()                #converts RDD tuple type to dictionary
#print(filter_w)


phrases=text.flatMap(splitter_phrase)
phrase_map=phrases.map(lambda x:(x,1))
counts_p=phrase_map.reduceByKey(add)
filter_p=counts_p.collectAsMap()
print(filter_p)



string=''
list=['education','canada','university','dalhousie','expensive','good','bad','good school','bad school','faculty','computer science','graduate']
print(filter_p['computer science'])
for i in list:
    try:
        if(filter_w[str(i)]):
            string+=str(i)
            string+=': '+str(filter_w[str(i)])+'\n'
    except:
        try:
            if(filter_p[str(i)]):
                string+=str(i)
                string+=': '+str(filter_p[str(i)])+'\n'
        except:
            string+=i+': 0\n'
            pass


f=open('../bin/myfiles/final_result_twitter.txt','w')
f.write(string)


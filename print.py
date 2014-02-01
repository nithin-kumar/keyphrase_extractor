#!/usr/bin/python
# File: print.py
# Author: Nithin Kumar KV
# Desc: This program demonstrates parallel computations with pp module
# It extract the keyphrases from different articles in parallel

import math, sys, time
import pp,json



def read_and_dump(doc): #create a master corpus from all the articles , and use these corpus for probability calculation
    return doc['content']



def clean_corpus(corpus):#Function to clean the corpus
    b = "*_=][{}1234567890!&@#$,.;:'?/-|+"
    corpus=corpus.lower()# Change to lowercase
    for i in range(0,len(b)):
        corpus=corpus.replace(b[i]," ")
    return corpus



def make_tables(corpus):#Function to build the unigram and bigram count tables
    from itertools import islice, izip
    import re
    import collections
    unigram_count={}
    bigram_count={}
    #store the counts of unigram in a hashtable,unigram_count
    unigrams=corpus.split()
    for word in unigrams:
        if word not in unigram_count:
            unigram_count[word]=1
        else:
            unigram_count[word]+=1
    #return unigram_count
     #store the counts of bigram in a hashtable,bigram_count
    words = re.findall("\w+", corpus)
    bigram_count=dict(collections.Counter(izip(words,islice(words, 1, None))))
    return unigram_count,bigram_count# Return the count tables



def extract_keyphrase(data,unigram_count,bigram_count):#Function to extract keyphrases from the given corpus
    from itertools import islice, izip
    import re
    import collections
    keyphrase={}#Hashtable to store the keyphrases and containg documents
    for item in data:#data is a list of dictinory
        article_id=item['id_id']
        article_content=item['content']
        article_content=clean_corpus(article_content)#read the text,and clean it
        #find bigrams from the cleaned text and check whether it is a valid keyphrase
        words = re.findall("\w+", article_content)
        bigram=list(collections.Counter(izip(words,islice(words, 1, None))))#bigram contains list of bigrams that we need to check
        for i,j in bigram:#i, is the first word and j is the second word
            if bigram_count[(i,j)]!=unigram_count[i]:
                #prob is -2*log of likelihood ratio 
                prob=float(-2*compute_likelihood(unigram_count[i],unigram_count[j],bigram_count[(i,j)],len(bigram_count)))
                if prob>20:# threshold value =20
                    if (i,j) not in keyphrase:
                        keyphrase[(i,j)]=[article_id]
                    else:
                        keyphrase[(i,j)].append(article_id)       
    key_id=1#id for keyphrase
    #Output
    print "KEYPHRASE ID            KEYPHRASE                          DOCUMENT ID"
    print "------------            ---------                          -----------"
    for i,j in keyphrase.keys():
        print key_id,"                    "+i+" "+j+"                        ",keyphrase[(i,j)]
        key_id+=1


#Algorithm to compute likelihood ratio
def compute_likelihood(unigram_count_i,unigram_count_j,bigram_count_i_j,len_):
    import math
    c12=bigram_count_i_j
    c1=unigram_count_i
    c2=unigram_count_j
    N=len_
    p=c2/float(N)
    p1=c12/float(c1)
    p2=(c2-c12)/float(N-c1)
    first=calculate(c12,c1,p)
    second=calculate(c2-c12,N-c1,p)
    third=calculate(c12,c1,p1)
    fourth=calculate(c2-c12,N-c1,p2)
    if first>0 and second>0 and third>0 and fourth>0:
        return math.log(first)+math.log(second)-math.log(third)-math.log(fourth)
    return 0


#function which calculate x^k*(1-x)^n-k
def calculate(k,n,x):
    if k>0 and n>k:
        first_tearm=x**k
        second_term=(1-x)**(n-k)
        return first_tearm*second_term
    return 0


#Read the data from the json file
data=json.load(open('data.json'))


# tuple of all parallel python servers to connect with
ppservers = ()
#ppservers = ("10.0.0.1",)

if len(sys.argv) > 1:
    ncpus = int(sys.argv[1])
    # Creates jobserver with ncpus workers
    job_server = pp.Server(ncpus, ppservers=ppservers)
else:
    # Creates jobserver with automatically detected number of workers
    job_server = pp.Server(ppservers=ppservers)

jobs = [(job_server.submit(read_and_dump, (input,))) for input in data]#Parallel computation to collect the corpus from different articles
list_corpus=[]#To collect all corpus 
for job in jobs:
    list_corpus.append(job())

master_corpus=""#corpus which contains all the articles from the given json file
for corpi in list_corpus:
    master_corpus+=corpi
#Now master corpus contains all the content 



#CORPUS Cleaning
clean= job_server.submit(clean_corpus, (master_corpus,) )
master_corpus=clean()#Parallel computation to do cleaning the master_corpus



# Build all unigram and bigram count tabels from the cleaned  master_corpus
buid_tables=job_server.submit(make_tables, (master_corpus,) )
unigram_count,bigram_count=buid_tables()

#Extrating keyphrases

final_job=job_server.submit(extract_keyphrase, (data,unigram_count,bigram_count,),(calculate,compute_likelihood),)
result=final_job()
print result

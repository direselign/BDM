
# coding: utf-8

# In[7]:


import re
from time import time
import numpy as np
from operator import add

data = sc.wholeTextFiles('dataset/reut2-*')
newsArticles = data.map(lambda x:x[1]).flatMap(lambda x:x.split('<BODY>')[1:]).map(lambda x:x.split('</BODY>')[0])                   .map(lambda x:re.sub(' +', ' ', x.replace('\n', ' ')))
newsArticles.take(3)


# ### Q#1. Given the Reuters-21578 dataset, please calculate all k-shingles and output the set representation of the text dataset as a matrix.

# In[8]:


#Enter the K value of K-Shingles from the user
k=int(input("Please Enter the K value of K-Shingles:- "))


# In[9]:


shingles = newsArticles.flatMap(lambda x:[x[i:i+k] for i in range(len(x)-k+1)]).distinct()
shingles_count = shingles.count()
articles_count = newsArticles.count()
print('different shingles-->',shingles_count)
print('different articles-->',articles_count)


# In[10]:


newsArticles = newsArticles.collect()
k_shingles_matrix = shingles.map(lambda s:[1 if s in a else 0 for a in newsArticles])
k_shingles_matrix.coalesce(1).saveAsTextFile('tmp_hw3')

'''
move the file from tmp_hw3 folder to hw_3_result folder and rename the file finally delete the tmp_hw3 folder
'''
get_ipython().system('mv tmp_hw3/part-00000 hw_3_result/k_shingles_matrix.txt')
get_ipython().system('rm -rf tmp_hw3')


# ### Q2 Given the set representation, compute the minhash signatures of all documents using MapReduce.

# In[11]:


def biggerThanNFirstPrime(N):
    p = 2
    while True:
        isPrime = True
        for i in range(2,p//2+1):
            if(p%i==0):
                isPrime = False
                break
        if isPrime and p > N:
            return p
        else:
            p+=1


# In[12]:


#Take the number of randum hash functions from the user
h=int(input("Please Enter the number of hash functions:- "))


# In[18]:


import random
a = [random.randint(0, h) for i in range(h)]
b = [random.randint(0, h) for i in range(h)]
p = biggerThanNFirstPrime(articles_count)
N = articles_count

def rowHash(row, a, b, p, N):
    return ((a*row+b)%p)%N


# In[23]:


from time import clock
minHashSignatures = list()
#intialize time
initial_time=time()
kShinglesMatrixZipWithIndex = k_shingles_matrix.zipWithIndex().cache()
for i in range(h):
    minHashSignatures.append(kShinglesMatrixZipWithIndex                             .map(lambda x:[rowHash(x[1], a[i], b[i], p ,N) if c == 1 else (articles_count + 10) for c in x[0]])                             .reduce(lambda x, y:[Mx if Mx < My else My for Mx, My in zip(x, y)]))


# In[ ]:


with open('hw_3_result/min_hash_signatures.txt', 'w') as result:
    for row in minHashSignatures:
        result.write(str(row) + '\n') 


# #### Q#3 Implement the LSH algorithm by MapReduce and output the resulting candidate pairs of similar documents.

# In[37]:


#please enter the band size from the user sample value 20
bands=int(input("Please Enter the band size:- "))


# In[ ]:


r = 5
similarRate = 0.8
buckets = articles_count
hashFuct = [[random.randint(0, 100) for i in range(r + 1)] for j in range(bands)]

with open('hw_3_result/candidate_pairs.txt', 'w') as result:
    for i in range(articles_count):
        candidatePairs = list()
        for j in range(bands):
            band = np.array(minHashSignatures[j*r:j*r+r]).T
            band = [(np.array(article).dot(np.array(hashFuct[j][:r])) + hashFuct[j][-1]) % buckets for article in band]
            for k, article in enumerate(band):
                if k > i and article == band[i]:
                    candidatePairs.append(k)
        candidatePairs = [(article, candidatePairs.count(article)) for article in set(candidatePairs)]
        candidatePairsTreshold = list()
        for candidatePair in candidatePairs:
            if candidatePair[1] >= bands*similarRate:
                candidatePairsTreshold.append(candidatePair[0])
        result.write('Articles' + str(i) + ':' + str(candidatePairsTreshold) + '\n')


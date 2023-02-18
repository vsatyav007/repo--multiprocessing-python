# -*- coding: utf-8 -*-
"""
@author: Satya
"""
import multiprocessing
import os
import pandas as pd
import time
import numpy as np

pdatawd = os.getcwd()+'/processeddata/'
df = pd.read_csv('wordpairs.csv')
v = df.values

def GetWordPairCountMatrix(filepath, filename):
    # print(filepath, filename)
    k = np.zeros((df.shape[0], 1), dtype=int)
    f1 = open(filepath, 'r', encoding='utf-8')
    for line in f1:
        line = line.strip()
        words = line.split()
        for i in range(df.shape[0]):
            if ((v[i, 0] in words) and (v[i, 1] in words)):
                k[i, 0] = k[i, 0] + 1
    f1.close()
    df2 = pd.DataFrame({filename: k[:, 0]})
    df2.to_csv(filename +'_pool.csv', index=False)

def FileTraversal(file_paths):
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 2)
    print("No. of CPU Cores ", multiprocessing.cpu_count() - 2)  # 6 cores
    for filepath, filename in file_paths:
        pool.apply_async(GetWordPairCountMatrix,
                         args=(filepath, filename,))
    pool.close()
    pool.join()

def appendresults():
    df3 = pd.read_csv('wordpairs.csv')
    for filename in os.listdir(os.getcwd()):
        if filename.endswith('pool.csv'):
            df4 = pd.read_csv(filename)
            df3 = pd.concat([df3, df4], axis=1)
    df3.to_csv('WordPairEventCountMatrix_MultiProcessing.csv', index=False)

if __name__ == '__main__':
    start = time.time()
    file_paths = []
    for subdir, dirs, files in os.walk(pdatawd):
        if not dirs:
            for filename in files:
                filepath = subdir + os.sep + filename
                if filepath.endswith('.txt'):
                    filepath = subdir + os.sep + filename
                    filename = os.path.splitext(filename)[0]
                    file_paths.append((filepath, filename))
    FileTraversal(file_paths)
    appendresults()
    print('time took:%s' % (time.time()-start))  # 1875 seconds = 31 mins # 1390
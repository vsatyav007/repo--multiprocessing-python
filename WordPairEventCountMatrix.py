# -*- coding: utf-8 -*-
"""
@author: Satya
"""
import os
import numpy as np
import pandas as pd
import time

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
                k[i, 0] = k[i, 0]+1
    f1.close()
    df2 = pd.DataFrame({filename: k[:, 0]})
    return df2

def FileTraversal(file_paths):
    for filepath, filename in file_paths:
        df[filename] = GetWordPairCountMatrix(filepath, filename)
    df.to_csv('WordPairEventCountmatrix.csv', index=False)
    print(df.shape)

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
    print('time took:%s' % (time.time()-start))

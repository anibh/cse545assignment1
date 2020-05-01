##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course
## Spring 2020
##
## Student Name: Anirban Bhattacharya
## Student ID: 112733959

##Data Science Imports:

import numpy as np
import mmh3
from random import random
from random import shuffle
from datetime import datetime

##IO, Process Imports:
import sys
from pprint import pprint


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent=.01, sample_col=0):
    # Implements the standard non-streaming sampling method
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random  1% of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn

    mean, standard_deviation = 0.0, 0.0
    userId = set()
    for line in filename:
        userId.add(line.split(',', 4).pop(sample_col))

    filename.seek(0)
    shuffle(list(userId))
    sample_userId = set(list(userId)[:int(len(userId) * percent)])
    values = list()
    sum = 0.0
    sum_sqr = 0.0
    for newLine in filename:
        elements = list()
        newLine = newLine.strip()
        elements = newLine.split(',', 4)
        if elements[2] in sample_userId:
            values.append(float(elements[3]))

    mean = np.mean(values)
    standard_deviation = np.std(values)

    ##<<COMPLETE>>

    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent=.01, sample_col=0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed.

    mean, standard_deviation = 0.0, 0.0
    count, sum, sum_sqr = 0.0, 0.0, 0.0
    ##<<COMPLETE>>
    n_chunks = int(1 / percent)
    chunk_number = int(random() * n_chunks)
    for line in stream:
        elements = line.strip().split(',', 4)
        if mmh3.hash(elements[2]) % n_chunks == chunk_number:
            count += 1
            sum = float(elements[3]) - mean
            mean += sum / count
            sum_sqr += sum * (float(elements[3]) - mean)
            if count >= 2:
                standard_deviation = np.sqrt(sum_sqr / (count - 1))
    ##<<COMPLETE>>
    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.C Timing

files = ['transactions_small.csv', 'transactions_medium.csv', 'transactions_large.csv']
percents = [0.01, 0.02, 0.005]

if __name__ == "__main__":

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            start_time = datetime.now()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            end_time = datetime.now()
            print("  Typical Sampler execution time: ", end_time - start_time)
            fstream.close()
            fstream = open(f, "r")
            start_time = datetime.now()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            end_time = datetime.now()
            print("  StreamSampler execuation time: ", end_time - start_time)

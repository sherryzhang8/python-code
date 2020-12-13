#!/bin/python3

import math
import os
import random
import re
import sys

# Complete the sockMerchant function below.
def sockMerchant(n, ar):
 #   ar_list = ar.split()
    color_list = []
    total_cnt = 0
    for i in range(0,n):
        pair = ar[i]
        if pair not in color_list:
            color_list.append(pair)
            cnt = 1
            for j in range(i+1,n):
                if pair == ar[j]:
                    cnt += 1
            total_cnt += int(cnt/2)
    return total_cnt   

if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    n = int(input())

    ar = list(map(int, input().rstrip().split()))

    result = sockMerchant(n, ar)

    fptr.write(str(result) + '\n')

    fptr.close()

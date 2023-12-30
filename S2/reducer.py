#!/usr/bin/python
import sys
import os
import logging

nl = os.linesep
logging.basicConfig(level=logging.INFO)

def cast_to_int(s: str)->int:
    try:
        price = int(s)
        return 0, price
    except:
        return 1, 0
         
n, mean, M2 = 0, 0.0, 0
with sys.stdin as stdin:
    for line in stdin:
        err_code, price = cast_to_int(line)
        logging.info(f'line: {line.strip()}, err_code: {err_code}, price: {price}')
        if err_code == 0:
            n += 1
            delta = price - mean
            mean += delta / n
            M2 += delta * (price - mean)
        else:
            continue
# print(mean, M2)
print(f'n={n}, mean={mean}, std_dev={(M2 / n) ** (1/2)}')
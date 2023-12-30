#!/usr/bin/python
import sys
import os

nl = os.linesep

with sys.stdin as stdin:
    for line in stdin:
        sys.stdout.write(f'{line.split(",")[9]}{nl}')

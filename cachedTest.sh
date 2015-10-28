#!/bin/bash

#Execute the Cached Persistent File System
nohup sh startFS.sh  &

#Test creation of a file
TIMEFORMAT=%R
T1 = time echo "abbbbbbbbbbbbbbb" >> file.txt

#Test creation of a directory

T2 = time mkdir test1

#Test creation of a hierarchy of directories

T3 = time mkdir test1/test2/test3/test4

#Test creation of a file under hierarchy
cd test1/test2/test3/test4
T4 = time echo "This a new file" >> file4.txt

#Test create symlink a file

T5 = time ln -s file4.txt file5.txt



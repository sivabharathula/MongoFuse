#!/bin/bash


#Create directory to mount a file
rmdir fusemount
mkdir fusemount

#Execute the Cached Persistent File Syste
python cachedPersistentFS.py fusemount

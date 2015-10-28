#!/usr/bin/env python

import logging
import os, sys
import xmlrpclib
import pickle
import uuid
import pymongo
import signal
import time
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from pymongo import MongoClient


def exit_gracefully(memory):
    if os.path.exists('./.fileSystemstateNocache.txt'):
        os.remove('./.fileSystemstateNocache.txt')
    f = open('./.fileSystemstateNocache.txt','wb')
    pickle.dump(memory,f)
    f.close()

if not hasattr(__builtins__, 'bytes'):
    bytes = str

client = MongoClient()
db = client.test_database


#directory object
class Directory(object):
    def __init__(self, name, metadata):
        self.children = {}
        self.metadata = metadata
        self.name = name
        self.datakey = str(uuid.uuid4())
#file object stores data bytes in dictionary
class File(object):
    def __init__(self,name,metadata):
        self.name = name
        self.metadata = metadata
        self.datakey = str(uuid.uuid4())
 

#File System class
class Memory(LoggingMixIn, Operations):
    def __init__(self):
        print 'init'
        self.fd = 0
        now = time()
        metadata = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,st_mtime=now, st_atime=now, st_nlink=2)
        self.root = Directory("/",metadata)

    #split path of directory for hierarchical access
    #code snippet reference: https://www.safaribooksonline.com/library/view/python-cookbook/0596001673/ch04s16.html
    def splitall(self,path):
        allparts = []
        while 1:
            parts = os.path.split(path)
            if parts[0] == path:  # sentinel for absolute paths
                allparts.insert(0, parts[0])
                break
            elif parts[1] == path: # sentinel for relative paths
                allparts.insert(0, parts[1])
                break
            else:
                path = parts[0]
                allparts.insert(0, parts[1])
        return allparts

    #lookup for a node
    def lookup(self,path):
        parts = self.splitall(path)
        node = self.root
        
        for x in parts:
            if x == '/':
                node = self.root
            else:
                if x not in node.children:
                    raise FuseOSError(ENOENT)
                else:
                    node = node.children[x]
        return node

    #create directory object as a child of looked up node
    def mkdir(self, path, mode):
        metadata = dict(st_mode=(S_IFDIR | mode), st_nlink=2,st_size=0, st_ctime=time(), st_mtime=time(),st_atime=time())
        head, tail = os.path.split(path)
        node = self.lookup(head)
        node.children[tail] = Directory(tail,metadata)

    #obtain attributes of a file
    def getattr(self, path, fh=None):
        node = self.lookup(path)
        return node.metadata

    #returns a file descriptor 
    def open(self, path, flags):
        self.fd += 1
        print "out" + str(self.fd)
        print self.fd

        return self.fd

    #read dir adds support for '.' and '..' 
    def readdir(self, path, fh):
        node = self.lookup(path)
        return ['.', '..'] + [x[0:] for x in node.children if x != '/']

    def chmod(self, path, mode):
        node = self.lookup(path)
        node.metadata['st_mode'] &= 0770000
        node.metadata['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        node = self.lookup(path)
        node.metadata['st_uid'] = uid
        node.metadata['st_gid'] = gid

    def create(self, path, mode):
        metadata = dict(st_mode=(S_IFREG | mode), st_nlink=1,st_size=0, st_ctime=time(), st_mtime=time(),st_atime=time())
        head, tail = os.path.split(path)
        node = self.lookup(head)
        node.children[tail] = File(tail,metadata)
        cData = defaultdict(bytes)
        pc = pickle.dumps(cData)
        record = {"name": node.children[tail].datakey, "data" : pc}
        db.files.insert_one(record)
        self.fd += 1
        return self.fd

    def getxattr(self, path, name, position=0):
        node = self.lookup(path)
        attrs = node.metadata.get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        node = self.lookup(path)
        attrs = node.metadata.get('attrs', {})
        return attrs.keys()

    def read(self, path, size, offset, fh):
        node = self.lookup(path)
        head, tail = os.path.split(path)
        rv1 = db.files.find_one({"name": node.datakey})
        curData1 = pickle.loads(rv1["data"])
        return curData1[node.datakey][offset:offset + size]

    def rmdir(self, path):
        head, tail = os.path.split(path)
        node = self.lookup(head)
        node.children.pop(tail)

    def setxattr(self, path, name, value, options, position=0):
        node = self.lookup(path)
        attrs = node.metadata.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def removexattr(self, path, name):
        node = self.lookup(path)
        attrs = node.metadata.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def symlink(self, target, source):
        head, tail = os.path.split(target)
        node = self.lookup(head)
        metadata = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,st_size=len(source))
        node.children[tail] = File(tail,metadata)
        node = self.lookup(target)
        curData = defaultdict(bytes) 
        curData[node.datakey] = source
        p = pickle.dumps(curData)
        record = {"name": node.datakey, "data" : p}
        db.files.insert_one(record)

    def truncate(self, path, length, fh=None):
        node = self.lookup(path)
        head, tail = os.path.split(path)
        node.metadata['st_size'] = length
        rv1 = db.files.find_one({"name": node.datakey})
        curData1 = pickle.loads(rv1["data"])
        curData1[node.datakey] = curData1[node.datakey][:length]
        p1 = pickle.dumps(curData1)
        db.files.replace_one({"name": node.datakey}, {"name": node.datakey, "data" : p1})

    def unlink(self, path):
        head, tail = os.path.split(path)
        node = self.lookup(head)
        node.children.pop(tail)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        node = self.lookup(path)
        node.metadata['st_atime'] = atime
        node.metadata['st_mtime'] = mtime

    def readlink(self, path):
        node = self.lookup(path)
        head, tail = os.path.split(path)
        rv1 = db.files.find_one({"name": node.datakey})
        curData1 = pickle.loads(rv1["data"])
        return curData1[node.datakey]

    def write(self, path, data, offset, fh):
        node = self.lookup(path)
        head, tail = os.path.split(path)
        rv1 = db.files.find_one({"name": node.datakey})
        curData1 = pickle.loads(rv1["data"])
        curData1[node.datakey] = curData1[node.datakey][:offset] + data
        p1 = pickle.dumps(curData1)
        db.files.replace_one({"name": node.datakey}, {"name": node.datakey, "data" : p1})
        node.metadata['st_size'] += len(data)
        return len(data)

    def rename(self, old, new):
        head, tail = os.path.split(old)
        oldparent = self.lookup(head)
        headn, tailn = os.path.split(new)
        newparent =  self.lookup(headn)
        newparent.children[tailn] = oldparent.children.pop(tail)

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    try:
        logging.getLogger().setLevel(logging.DEBUG)
        memory = Memory()
        if os.path.exists('./.fileSystemstateNocache.txt'):
            f1 = open('./.fileSystemstateNocache.txt','rb')            
            memory = pickle.load(f1)
        fuse = FUSE(memory, argv[1], foreground=True,debug=True)
    except KeyboardInterrupt:
        pass
    finally:
        exit_gracefully(memory)

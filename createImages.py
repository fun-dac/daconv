# -*- coding: utf-8 -*-
import zoomify_im
import pymongo
import datetime
import os.path
import uuid
import shutil
import sys
import time
import logging
import logging.handlers

class Zconvd:
    def __init__(self, dbName, colQue, colImg):
        self.dbName = dbName
        self.colQue = colQue
        self.colImg = colImg

    def connect(self):
        self.conn = pymongo.Connection()
        self.col_que = self.conn[self.dbName][self.colQue]
        self.col_img = self.conn[self.dbName][self.colImg]

    def disconnect(self):
        self.conn.disconnect()
    
    def convert(self):
        target = self.col_que.find_one({"status": "waiting"})
        
        if target is None:
            return
        else:
            target["status"] = "processing"
            targetID = str(target["_id"])
            self.col_que.save(target)
        
        inputPath = target["path"]
        inputBase = os.path.basename(inputPath)
        inputFilename = inputBase.split(".")[0]
        outputDir = target["outputDir"]
        outputPath = outputDir + target["flatName"]

        if os.path.exists(outputPath):
            print ("OutputFile already exsits")
            self.col_que.remove(target)
        
            finTime = str(datetime.datetime.now())
            imgs = {"groupID": target["groupID"],
                    "created_at": finTime,
                    "path": outputPath+"/TileGroup0/0-0-0.jpg",
                    "zoomifyPath": outputPath,
                    "folderName": target["folderName"],
                    "flatName": target["flatName"],
                    "relName": target["relName"],
                    }
            self.col_img.save(imgs)

#---main---
z = Zconvd("archives","queues", "images")
z.connect()
while 1:
    z.convert()
    time.sleep(1)
z.disconnect()

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

        self.log = logging.getLogger("MyLogger")
        self.log.setLevel(logging.DEBUG)
        handler = logging.handlers.RotatingFileHandler("zconvd.log",
                maxBytes=1024*1024,#1MB
                backupCount=3)
        self.log.addHandler(handler)
        self.log.info("["+str(datetime.datetime.now())+"]----zconvd init----")

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

        if not os.path.exists(inputPath):
            target["status"] = "failed"
            self.col_que.save(target)
            self.log.error("["+str(datetime.datetime.now())+"]"+
                    " InputFile not exists (id:"+targetID+" path:"+ target["path"]+")")
#            raise Exception("InputFile not exists")
            print ("InputFile not exists")
            return
        elif os.path.exists(outputPath):
            target["status"] = "failed"
            self.col_que.save(target)
            self.log.error("["+str(datetime.datetime.now())+"]"+
                    " OutputFile already exsits (id:"+targetID+
                    " path:"+target["path"]+")")
#            raise Exception("OutputFile already exists")
            print ("OutputFile already exsits")
            return

        #変換中に生成される一時ファイルが一意でないので
        #一度一意な作業ディレクトリを作ってその中で変換
        tmpDir = outputDir + "TMP-" + str(uuid.uuid4()) + "/"
        os.makedirs(tmpDir)
        tmpInputPath = tmpDir + inputBase
        tmpOutputPath = tmpDir + target["flatName"]
        shutil.copy(inputPath, tmpInputPath)

        try:
            self.log.info("["+str(datetime.datetime.now())+"] Start conv (id:"+
                    targetID+" path:"+target["path"]+")")
            zm = zoomify_im.ZoomifyIM(tmpInputPath, None, None, 256, tmpOutputPath)
            zm.generateTileSet()
        except:
            target["status"] = "failed"
            self.col_que.save(target)
            self.log.error("["+str(datetime.datetime.now())+"]"+
                    " ZoomifyIM processing error (id:"+targetID+
                    " path:"+ target["path"]+")")
#            raise Exception("ZoomifyIM processing error")
            print("ZoomifyIM processing error")
            return

        os.rename(tmpOutputPath, outputPath)
        shutil.rmtree(tmpDir)
        os.chmod(outputPath, 0777)

        #変換成功したらキューから削除
        self.col_que.remove(target)

        finTime = str(datetime.datetime.now())
        #imagesに書き込み
        imgs = {"groupID": target["groupID"],
                "created_at": finTime,
                "path": outputPath+"/TileGroup0/0-0-0.jpg",
                "zoomifyPath": outputPath,
                "folderName": target["folderName"],
                "flatName": target["flatName"],
                "branchNum": target["branchNum"],
                "originalFileSet": target["originalFileSet"],
                "relName": target["relName"],
                }
        self.col_img.save(imgs)

        print("["+finTime+" Finish conv] id:"+targetID+" path:"+target["path"])
        self.log.info("["+finTime+"] +--Finish conv (id:"+targetID+
                " path:"+ target["path"]+")")

#---main---
argvs = sys.argv
if (len(argvs) != 2):
    print 'Usage: # python %s db-name' % argvs[0]
    quit()

print 'target db: %s' % argvs[1]
z = Zconvd(argvs[1], "queues", "images")
z.connect()
while 1:
    z.convert()
#    try:
#        z.convert()
#    except Exception:
#        print("raise exception")
    time.sleep(1)
z.disconnect()

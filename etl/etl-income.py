# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 17:22:28 2016

@author: FinArb
"""
#declare libraries
import os
import sys
import time
import urllib2
import requests
import pandas as pd
#define root path
root=str(sys.arv[1])
#take folder separator as per os
pathspt=os.path.sep
# path of zip files
gdllink='http://globaldatalab.org/areadata/downloads/GDLAreaData140.csv'

#check for extraction directories existence
if not os.path.isdir(root+pathspt+'downloaded'):
    os.makedirs(root+pathspt+'downloaded')
    
if not os.path.isdir(root+pathspt+'files'):
    os.makedirs(root+pathspt+'files')
    
    
#function to download
def downloadFile(url, directory) :
    localFilename = url.split(pathspt)[-1]
    with open(directory + pathspt + localFilename, 'wb') as f:
        start = time.clock()
        r = requests.get(url, stream=True)
        total_length = r.headers.get('content-length')
        dl = 0
        if total_length is None: # no content length header
            f.write(r.content)
        else:
            for chunk in r.iter_content(1024):
                dl += len(chunk)
                f.write(chunk)
                done = int(50 * int(dl) / int(total_length))
                sys.stdout.write("\033[K")
                sys.stdout.write("\r[%s%s] %s bps\r" % ('=' * done, ' ' * (50-done), dl//(time.clock() - start)))
                print ''
    return (time.clock() - start)
    
# download the file
outputFilename = root+ pathspt+"downloaded"+pathspt + "GDLAreaData140.csv"
if (not os.path.isfile(outputFilename)):
    time_elapsed = downloadFile(gdllink,root+pathspt+"downloaded")
    print "Download complete..."
    print "Time Elapsed: " + str(time_elapsed)
    req = urllib2.Request(gdllink)
    url_handle = urllib2.urlopen(req)
    headers = url_handle.info()
    etag = headers.getheader("ETag")
    last_modified = headers.getheader("Last-Modified")
    last_modified=last_modified[:-4]
    last_modified=last_modified.replace(",", "", 1)
    text_file = open(root+pathspt+"downloaded"+pathspt+"Zipmd.txt", "w")
    text_file.write(last_modified)
    text_file.close()
else:
    req = urllib2.Request(gdllink)
    url_handle = urllib2.urlopen(req)
    headers = url_handle.info()
    etag = headers.getheader("ETag")
    last_modified = headers.getheader("Last-Modified")
    last_modified=last_modified[:-4]
    last_modified=last_modified.replace(",", "", 1)
    file = open(root+pathspt+"downloaded"+pathspt+"Zipmd.txt", 'r')
    extime=file.read()
    zlmt=time.strptime(last_modified,'%a %d %b %Y %X')
    flmt=time.strptime(extime,'%a %d %b %Y %X')
    if ((time.mktime(zlmt)-time.mktime(flmt))>0):
        time_elapsed = downloadFile(gdllink,root+pathspt+"downloaded")
        print "Download complete..."
        print "Time Elapsed: " + str(time_elapsed)
        req = urllib2.Request(gdllink)
        url_handle = urllib2.urlopen(req)
        headers = url_handle.info()
        etag = headers.getheader("ETag")
        last_modified = headers.getheader("Last-Modified")
        last_modified=last_modified[:-4]
        last_modified=last_modified.replace(",", "", 1)
        text_file = open(root+pathspt+"downloaded"+pathspt+"Zipmd.txt", "w")
        text_file.write(last_modified)
        text_file.close()

#read data into pandas data frame
data=pd.read_csv(root+pathspt+"downloaded"+pathspt+"GDLAreaData140.csv")
data.drop('datasource', axis=1, inplace=True)
data.set_index(['iso_code','ISO2','country','year','GDLCODE','level','region'], inplace=True)
colname=data.columns
for i in range(0,data.shape[1]):
    tempdf=data[[colname[i]]]
    tempdf.to_csv(root+pathspt+'files'+pathspt+'temp.csv')
    tempdf=pd.read_csv(root+pathspt+'files'+pathspt+'temp.csv')
    tempdf.to_csv(root+pathspt+'files'+pathspt+colname[i]+'.csv',index=False)
    os.remove(root+pathspt+'files'+pathspt+'temp.csv')
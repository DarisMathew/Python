from io import BytesIO
from urllib.request import urlopen
import urllib
from zipfile import ZipFile
import pandas as pd
import time
import re
import os
import glob 

currentDir = os.getcwd()
zipURLList = []
data_frames = []
oldCount = 0
newCount = 0

url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
regex = '(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+'

def remove_duplicates_list(urlList):
    urlList = list(dict.fromkeys(urlList))
    return urlList

pattern = currentDir + "/*.csv"
csvFileName = currentDir + "\\one_giant_file.csv"
f = open(csvFileName,'w')

starttime = time.time()

while True:
    urllib.request.urlretrieve(url,'dataLink.txt')
    
    oldCount = len(zipURLList)
    
    with open("dataLink.txt") as file:
        zipURLList.append(re.findall(regex, file.readline())[0])
    zipURLList = remove_duplicates_list(zipURLList)
    newCount = len(zipURLList)

    if oldCount > 0 and newCount > oldCount :
#         print("isUpdated")
        with urlopen(zipURLList[0]) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall(currentDir)   
                      
        csvFile = glob.glob(pattern)
        existingDf = pd.read_csv(csvFileName)
        data_frames.append(existingDf)
        del existingDf
        
        newDf = pd.read_csv(csvFile[0], sep='\t', engine='python')
        data_frames.append(newDf)
        del newDf
    else:
        with urlopen(zipURLList[0]) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall(currentDir) 
            
        csvFile = glob.glob(pattern)
        df = pd.read_csv(csvFile[0], sep='\t', engine='python')
        
    data_frames.append(df)
    del df
    os.remove(csvFile[0])
    
    big_un = pd.concat(data_frames, ignore_index=True,sort=False)
    big_un.drop_duplicates(inplace=True)
    big_un.to_csv(csvFileName)
        
    df = pd.read_csv(csvFileName)
    df.to_parquet('output.parquet')
    
    time.sleep((15*60) - ((time.time() - starttime) % (15*60)))

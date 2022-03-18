# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 21:28:14 2022

@author: IvDesign
"""
import os
import sys
from threading import Thread
from typing import List, Dict
import time

from multiprocessing import Queue

from workers.TheadedS2Downloader import S2DownloadScheduler


def main(href: List, download_workers: int = 4):
    download_start_time = time.time()
    
    href_queue = Queue() # input queue
    array_queue = Queue() # output queque
    
    # Add links to input queue
    for link in href:
        href_queue.put(link)
    
    # List which adds 
    href_download_threads = []
    
    # Get cpu count
    max_cpu = os.cpu_count() - 1
    
    # Set amount of thread workers
    if len(href) < download_workers:
        download_workers = len(href)
    elif download_workers > max_cpu:
        download_workers = max_cpu
    else:
        download_workers = download_workers
    
    # Start downloader                      
    for i in range(download_workers):
        DownloaderScheduler = S2DownloadScheduler(href_queue, array_queue)
        href_download_threads.append(DownloaderScheduler)

    # Add a stop item to the queue
    for i in range(len(href_download_threads)):
        href_queue.put('DONE')
        time.sleep(1)

    # Join the threads
    for i in range(len(href_download_threads)):
        href_download_threads[i].join()
    
    # Dict to store results from queue
    result_dict = {}
    
    # Iterate output queue
    try:
        while True:
            result = array_queue.get()
            if result == "DONE":
                break
            # Add results to dict
            result_dict.update(result)
    except:
        print("Queue failed")
        
        
    print(result_dict.keys())
    
    for key in result_dict.keys():
        print(f"Band {key}: {result_dict[key].keys()}")
        

    print('Download took:', round(time.time() - download_start_time, 1))



if __name__ == "__main__":
    bucket = 'https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/22/X/EG/2022/3/S2B_22XEG_20220317_0_L2A/'
    bandLst = ['B02.tif', 'B03.tif', 'B04.tif']
    hrefLst = [f'{bucket}{band}' for band in bandLst]
    main(hrefLst)
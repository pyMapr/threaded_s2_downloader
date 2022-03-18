# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 21:02:21 2022

@author: IvDesign
"""

from queue import Empty
import threading

import numpy as np
try:
    import gdal
except:
    from osgeo import gdal
    

class S2DownloadScheduler(threading.Thread):
    def __init__(self, input_queue, output_queue, **kwargs):
        
        super(S2DownloadScheduler, self).__init__(**kwargs)
        self._input_queue = input_queue
        temp_queue = output_queue
        if type(temp_queue) != list:
            temp_queue = [temp_queue]
        self._output_queues = temp_queue
        self.start()
           
            
    def run(self):        
        while True:
            try:
                link = self._input_queue.get(timeout=60)
            except Empty:
                print('Download scheduler queue is empty, stopping')
                break
            if link == 'DONE':
                for output_queue in self._output_queues:
                    output_queue.put('DONE')
                break
            
            s2DownloadWorker = S2DownloadWorker(link)
            image_info = s2DownloadWorker.read_mem()
            
            for output_queue in self._output_queues:
                output_queue.put(image_info)

            

class S2DownloadWorker():
    def __init__(self, href, **kwargs):
        self._href = href
    
    def read_mem(self):
        
        vsi_link = self._build_links()
        band_name = self._get_band_name()
        
        src = gdal.Open(vsi_link)
        projection = src.GetProjection()
        geotransform = src.GetGeoTransform()
        img_array = src.ReadAsArray()
        src = None
        
        return {band_name: {"array": img_array, "prj": projection, "gt": geotransform}}
        
    
    def _build_links(self):
        #print('built link', f"/vsicurl/{self._href}")
        return f"/vsicurl/{self._href}"
    
    def _get_band_name(self):
        return self._href.split('/')[-1][:-4]
        

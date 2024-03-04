#!/bin python3

from rosbags.highlevel import AnyReader
from pathlib import Path
import os
import matplotlib.pyplot as plt
import numpy as np
from multiprocessing import Pool
from cv_bridge import CvBridge
import copy
import tqdm
import cv2
class BaseExtractor:
    
    def __init__(self, ros_bag, cores=4):
        self.ros_bag = ros_bag
        self.cores = cores

    def loop(self, filter_func, action_func):

        with AnyReader([Path(self.ros_bag)]) as reader:
            connections = filter(filter_func, reader.connections)
            return map(action_func(reader), reader.messages(connections=connections))

    def loop_reader(self, filter_func, action_func):
        with Pool(self.cores) as p:
            with AnyReader([Path(self.ros_bag)]) as reader:
                connections = p.filter(filter_func, reader.connections)
                act = action_func(reader)
            return list(p.map(act, reader.messages(connections)))

    def print_messages(self, filter_func):
        
        with AnyReader([Path(self.ros_bag)]) as reader:
            connections = filter(filter_func, reader.connections)
            for connection, timestamp, rawdata in reader.messages(connections):
                if(filter_func(connection)):
                    msg = reader.deserialize(rawdata, connection.msgtype)
                    print(f"topic : {connection.topic}| msg encoding : {msg}|\n\n")
    
    def get_topics(self):
        with AnyReader([Path(self.ros_bag)]) as reader:
            return [connection.__dir__() for connection in reader.connections]

    def print_topics(self):
        topics = self.get_topics()
        print("Topics in the Bag are : ")
        for topic in topics:
            print(topic)
    
    def print_info(self):
        print("General Info: \n Topic \t| msgtype \t| msg_count \t")
        with AnyReader([Path(self.ros_bag)]) as reader:
            
            for connection in reader.connections:
                self.print_connection_info(connection)

    def print_connection_info(self, connection):
        print(f"{connection.topic}|{connection.msgtype}|{connection.msgcount}")

def print_messages(reader):
    
    def action(connection_info):
        return reader.deserialize(connection_info[2], connection_info[0].msgtype)
    return action
        


def get_timestamps(connection):
    return connection[1]



class ImageExtractor(BaseExtractor):
    
    def __init__(self, ros_bag, cores, img_save_path):
        super().__init__(ros_bag, cores)
        self.img_save_path = img_save_path
        os.makedirs(img_save_path, exist_ok=True)
        self.cv_bridge = CvBridge()
        
    def extract(self, topics):
        topic_dict = {}
        for topic in topics:
            dir_name = topic.replace("/", "_")
            topic_dict[topic] = os.path.join(self.img_save_path, dir_name)
            os.makedirs(topic_dict[topic], exist_ok=True)
        with Pool(self.cores) as pool:
            with AnyReader([Path(self.ros_bag)]) as reader:
                connections = filter(self.topics_filter(topics), reader.connections)
                for connection, timestamp, rawdata in tqdm.tqdm(reader.messages(connections)):
                    msg = reader.deserialize(rawdata, connection.msgtype)
                    self.save_img(msg, connection.msgtype, os.path.join(topic_dict[connection.topic], f'{timestamp}.png'))
                
        
    
    def save_img(self, msg, msgtype, file_name):
        if msgtype == "sensor_msgs/msg/CompressedImage":
            img_data = self.cv_bridge.compressed_imgmsg_to_cv2(msg)
        else:
            img_data = self.cv_bridge.imgmsg_to_cv2(msg, desired_encoding='passthrough')
        cv2.imwrite(file_name, img_data)
    
    @staticmethod                
    def topics_filter(topics):
        def filter(connection):
            if connection.topic in topics:
                return True
            else:
                return False
        return filter


topics = [
    "/color/image/compressed",
    #"/stereo/depth/compressedDepth",
    #"/imu/data",
    "/stereo/depth",
    #"/stereo/converted_depth"
]

ros_bag_path = os.path.join("/ros_bags/NorthFarm25/ros_bag1")


extractor = ImageExtractor(ros_bag_path, 4, "NorthFarmExtracted/RosBag1")

extractor.extract(topics)
#time_stamps_image_compressed = extractor.loop(topics_filter(["/gps/fix"]), get_timestamps)
#time_stamps_stereo_compressed = extractor.loop(topics_filter(["/stereo/points"]), get_timestamps)


# plt.figure()
# # #plt.hist(time_stamps_image_compressed, 30, label="gps/fix", alpha=0.5)
# # #plt.hist(time_stamps_stereo_compressed, 30, label="points", alpha=0.5)
# # #plt.xlabel("Time Stamps")
# # #plt.ylabel("Count")
# # #plt.grid()
# # #plt.legend()
# # plt.savefig("/workspaces/SKID_STEER/time_stamps.png")
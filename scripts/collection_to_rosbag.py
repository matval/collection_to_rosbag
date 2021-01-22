#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 13:48:17 2021

@author: mateus
"""
import os
import cv2 
import argparse
import pandas as pd
import numpy as np

import rospy
import rosbag
from sensor_msgs.msg import CompressedImage, Imu, NavSatFix, LaserScan
from cv_bridge import CvBridge, CvBridgeError

def main(args):
    system_log = os.path.join(args.collection, 'datalog/system_log')
    perception_lidar_log = os.path.join(args.collection, 'lidar/perception_lidar_log')
    lidar_log = os.path.join(args.collection, 'lidar/lidar_log')
    cam_front = os.path.join(args.collection, 'cam_front/cam_front.txt')
    #zf = zipfile.ZipFile(args.collection)
    bridge = CvBridge()
    
    if args.output == None:
        name_array = args.collection.split('/')
        i=1
        while len(name_array[-i]) == 0:
            i+=1
        output_name = name_array[-i] + '.bag'
    else:
        output_name = args.output
    
    with rosbag.Bag(output_name, 'w') as bag:
        '''
        ## Save system log part
        # read system_log
        df = pd.read_csv(zf.open('datalog/system_log'))
        df = pd.read_csv(system_log)
    
        for row in range(df.shape[0]):
            timestamp = rospy.Time.from_sec(df['(0)linux time (ms)'][row]/1000.0)
            imu_msg = Imu()
            imu_msg.header.stamp = timestamp
    
            # Populate the data elements for IMU
            imu_msg.angular_velocity.x = df['a_v_x'][row]
    
            bag.write("/imu", imu_msg, timestamp)
    
            gps_msg = NavSatFix()
            gps_msg.header.stamp = timestamp
    
            # Populate the data elements for GPS
            bag.write("/gps", gpu_msg, timestamp)
        '''
        # Save perception
        df = pd.read_csv(perception_lidar_log, header=1)
        #print('df:', df)
        #df = pd.read_csv(zf.open('lidar/perception_lidar_log'), header=1)
        initial_perception_ts = 0
        initial_lidar_ts = 0
        if(df.shape[0] > 0 and len(df['lidar_ts_ms']) > 0):
            initial_perception_ts = df['timestamp'][0]/1000.0
            initial_lidar_ts = df['lidar_ts_ms'][0]/1000.0
    
        for row in range(df.shape[0]):
            time_sec = df['timestamp'][row]/1000.0
            #print('frame time:', time_sec)
            timestamp = rospy.Time.from_sec(time_sec)
            imu_msg = Imu()
            imu_msg.header.stamp = timestamp
            
            if 'GyroVRoll' in df.columns:
                # Populate the data elements for IMU
                imu_msg.angular_velocity.x = df['GyroVRoll'][row]
                imu_msg.angular_velocity.y = df['GyroVPitch'][row]
                imu_msg.angular_velocity.z = df['GyroVYaw'][row]
                imu_msg.linear_acceleration.x = df['AccelerometerX'][row]
                imu_msg.linear_acceleration.y = df['AccelerometerY'][row]
                imu_msg.linear_acceleration.z = df['AccelerometerZ'][row]
                '''
                imu_msg.orientation.w = df['AccelerometerX'][row]
                imu_msg.orientation.x = df['AccelerometerX'][row]
                imu_msg.orientation.y = df['AccelerometerX'][row]
                imu_msg.orientation.z = df['AccelerometerX'][row]
                '''
    
                bag.write("/terrasentia/imu", imu_msg, timestamp)
            
            if 'GPSLatitude' in df.columns:
                gps_msg = NavSatFix()
                gps_msg.header.stamp = timestamp
        
                # Populate the data elements for GPS
                gps_msg.latitude = df['GPSLatitude'][row]
                gps_msg.longitude = df['GPSLongitude'][row]
                bag.write("/terrasentia/gnss", imu_msg, timestamp)
        
        # Save lidar log
        df = pd.read_csv(lidar_log, header=None)
        #print('df:', df)
    
        for row in range(df.shape[0]):
            time_sec = df.loc[row,0]/1000.0 - initial_lidar_ts + initial_perception_ts
            timestamp = rospy.Time.from_sec(time_sec)
            laser_msg = LaserScan()
            laser_msg.header.stamp = timestamp
            laser_msg.header.frame_id = 'laser'
    
            # Populate the data elements for IMU
            #ranges = line[1:-1]/1000.0
            ranges = df.loc[row,1:1081].values/1000.0
            #print('ranges:', ranges)
            laser_msg.ranges = ranges
            laser_msg.angle_min = -2.3562
            laser_msg.angle_max = 2.3562
            laser_msg.angle_increment = 0.004363323
            laser_msg.range_max = 30.0
            laser_msg.scan_time: 0.0250000003725
            laser_msg.range_min: 0.019999999553
    
            bag.write("/terrasentia/scan", laser_msg, timestamp)
            
        # Save images
        df = pd.read_csv(cam_front)
        #df = pd.read_csv(zf.open('cam_front/cam_front.txt'))
        # Path to video file 
        vidObj = cv2.VideoCapture(os.path.join(args.collection, 'cam_front/cam_front.mp4'))
        
        success = 1
        row = 0
        
        while success:
            # vidObj object calls read 
            # function extract frames 
            success, image = vidObj.read()
            
            if(not success):
                break
            
            print('frame:', row, '/', len(df['capture time (ms)']))
            #print('timestamp size:', len(df['capture time (ms)']))
            #print('video size:', vidObj.get(cv2.CAP_PROP_FRAME_COUNT))
            
            time_sec = df['capture time (ms)'][row]/1000.0
            #print('frame time:', time_sec)
            timestamp = rospy.Time.from_sec(time_sec)
            image_msg = CompressedImage()
            
            #print('image:\n', image)
            # Populate the data elements for Image
            image_msg = bridge.cv2_to_compressed_imgmsg(image)
            image_msg.header.stamp = timestamp
            
            bag.write("/terrasentia/usb_cam_node/rotated/compressed", image_msg, timestamp)
            row += 1
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", help='collection path')
    parser.add_argument("--output", help='output path and name')

    args = parser.parse_args()

    main(args)
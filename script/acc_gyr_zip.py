#!/usr/bin/env python
# coding: utf-8
import argparse
import pandas as pd

parser = argparse.ArgumentParser()

parser.add_argument('-f1','--file1', help='first file to merge')
parser.add_argument('-f2','--file2', help='secodnd file to merge')
parser.add_argument('-f2o','--fileo', help='output file')

in_acc_file = parser.file1
in_gyr_file = parser.file2
out_combined_file = parser.fileo

names = "Index,Arrival_Time,Creation_Time,x,y,z,User,Model,Device,gt,Type".split(',')


acc = pd.read_csv(in_acc_file,names = names,na_filter = None)
acc['Type'] = 'ACC'
acc = acc.astype({'Arrival_Time':int})

gyr = pd.read_csv(in_gyr_file,names = names,na_filter = None)
gyr['Type'] = 'GYR'
gyr = gyr.astype({'Arrival_Time':int})

accgyr = acc.append(gyr)
accgyr = accgyr.sort_values("Arrival_Time")
accgyr.to_csv(out_combined_file, encoding='utf-8', index=False)

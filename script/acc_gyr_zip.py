#!/usr/bin/env python
# coding: utf-8

import pandas as pd

in_acc_file = "../data/acc03.csv"
in_gyr_file = "../data/gyr03.csv"
out_combined_file = "../data/accgyr03.csv"

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

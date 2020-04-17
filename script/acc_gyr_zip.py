#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

in_acc_file = "acc03.csv"
in_gyr_file = "gyr03.csv"
out_combined_file = "accgyr03.csv"

names = "Index,Arrival_Time,Creation_Time,x,y,z,User,Model,Device,gt,Type".split(',')


acc = pd.read_csv(in_acc_file,names = names)
acc['Type'] = 'ACC'
acc = acc.astype({'Arrival_Time':int})

gyr = pd.read_csv(in_gyr_file,names = names)
gyr['Type'] = 'GYR'
gyr = gyr.astype({'Arrival_Time':int})

accgyr = acc.append(gyr)
accgyr = accgyr.sort_values("Arrival_Time")
accgyr.to_csv(out_combined_file, encoding='utf-8', index=False)


# In[ ]:





1. Human activity recognition from IMU

- Project structure (training + streaming) app
___
2. Dataset schema

- http://archive.ics.uci.edu/ml/datasets/Heterogeneity+Activity+Recognition
```
Index,Arrival_Time,Creation_Time,x,y,z,User,Model,Device,gt
0,1424696633908,1424696631913248572,-5.958191,0.6880646,8.135345,a,nexus4,nexus4_1,stand
1,1424696633909,1424696631918283972,-5.95224,0.6702118,8.136536,a,nexus4,nexus4_1,stand
2,1424696633918,1424696631923288855,-5.9950867,0.6535491999999999,8.204376,a,nexus4,nexus4_1,stand
.
.
```
___
3. Time series classification
___
4. Preprocessing

- SparkSql as state of the art
- DAG of stages
- Partitioning
___
5. Training with mllib pipeline
- DT vs MLP
___
6. SparkStreaming

- Windows batch
___
7. AWS deployment
- benchmarks
___
8. Challenges and future development
- optimization (GC, groupby vs reducebykey)
- code refactoring
- local vs cloud deploy
# Aisle Benchmark Results

## Testing data

We have two kinds of test data, one is ordered data, and the other is completely out of order data.

## Read No Page Performance

### Ordered Data
Performance comparison when no pages need to be read (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|   1M(10M)       |         3.7590    |      0.0462 |	      3.3641	    |	      0.0564	  |
|    2M(20M)      |         7.2606    |      0.0423 |	      6.7183	    |	      0.0704	  |
|    4M(40M)      |        14.7473    |      0.0440 |	     13.7198	    |	      0.0934	  |
|    8M(80M)      |        30.1360    |      0.0480 |	     27.5600	    |	      0.1680	  |
|   16M(160M)     |        59.4897    |      0.0619 |	     55.3041	    |	      0.2990	  |
|   32M(325M)     |       119.6585    |      0.0732 |	    109.8374	    |	      0.5285	  |
|    64M(649M)    |       238.1622    |      0.1149 |	    220.0135	    |	      0.9595	  |
|   128M(1.3G)    |       478.8200    |      0.1933 |	    441.8933	    |	      1.8533	  |


### Random Data
Performance comparison when no pages need to be read (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|    1M(14M)      |         3.7407    |      0.0423 |	      3.4974	    |	      0.0529	  |
|    2M(30M)      |         7.7876    |      0.0466 |	      7.0104	    |	      0.0674	  |
|    4M(60M)      |        15.9328    |      0.0420 |	     14.4454	    |	      0.1008	  |
|    8M(125M)     |        31.7468    |      0.0506 |	     28.8734	    |	      0.1709	  |
|   16M(256M)     |        63.7376    |      0.0638 |	     57.9929	    |	      0.2979	  |
|   32M(532M)     |       127.8994    |      0.0755 |	    115.7421	    |	      0.5283	  |
|   64M(1.1G)     |       257.0336    |      0.1141 |	    233.4295	    |	      0.9597	  |
|  128M(2.25G)    |       520.8039    |      0.1895 |	    471.4902	    |	      1.8627	  |


## Equivalent Query  Performance
### Ordered Data
Performance comparison with ordered data (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|   1M(10M)       |         5.7486    |      5.4973 |	      5.1803	    |	      0.8361	  |
|    2M(20M)      |        11.2995    |      5.9036 |	     10.4619	    |	      0.9492	  |
|    4M(40M)      |        22.4779    |      6.0074 |	     20.5882	    |	      0.9926	  |
|    8M(80M)      |        45.2373    |      5.9407 |	     41.6102	    |	      1.1186	  |
|   16M(160M)     |        86.2993    |      6.0680 |	     80.1837	    |	      1.1701	  |
|   32M(325M)     |       181.5905    |      6.0286 |	    168.7143	    |	      1.4381	  |
|    64M(649M)    |       340.8466    |      5.6705 |	    319.2045	    |	      1.7898	  |
|   128M(1.3G)    |       652.1447    |      5.5786 |	    611.5472	    |	      2.7170	  |


### Random Data
Performance comparison with totally random data (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|     1M(14M)      |        15.3671    |     15.3165 |	     14.9177	    |	     15.9620	  |
|     2M(30M)      |        30.7557    |     30.7045 |	     29.6477	    |	     31.7443	  |
|     4M(60M)      |        62.6422    |     62.5780 |	     60.2018	    |	     65.2018	  |
|     8M(125M)     |       120.0268    |    119.5369 |	    115.5436	    |	    123.4832	  |
|    16M(256M)     |       243.7778    |    242.8651 |	    235.1667	    |	    251.6349	  |
|    32M(532M)     |       515.8410    |    514.8615 |	    498.1692	    |	    527.7641	  |
|    64M(1.1G)     |       950.9029    |    949.2457 |	    916.1200	    |	    978.1314	  |
|   128M(2.25G)    |      1876.2157    |   1865.3725 |	   1819.9608	    |	   1927.6176	  |

## Average Read Performance

### Ordered Data
Performance comparison with ordered data (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|   1M(10M)       |         6.7037    |      6.4074 |	      6.0093	    |	      3.1389	  |
|    2M(20M)      |        13.5426    |     10.3298 |	     12.7447	    |	      6.2394	  |
|    4M(40M)      |        26.5282    |     16.7606 |	     24.7887	    |	     11.7465	  |
|    8M(80M)      |        48.1467    |     23.2133 |	     45.1333	    |	     18.1333	  |
|   16M(160M)     |       101.0506    |     51.2247 |	     94.8315	    |	     44.3764	  |
|   32M(325M)     |       214.3759    |     96.8014 |	    192.1418	    |	     80.7376	  |
|    64M(649M)    |       375.0408    |    164.0255 |	    351.4541	    |	    151.2296	  |
|   128M(1.3G)    |       740.0068    |    307.3197 |	    691.7279	    |	    287.6667	  |

### Random Data
Performance comparison with totally random data (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|     1M(14M)      |        26.2423    |     26.2835 |	     25.5464	    |	     27.0103	  |
|     2M(30M)      |        51.0270    |     50.9797 |	     49.6554	    |	     51.8311	  |
|     4M(60M)      |       102.7304    |    103.3217 |	    100.2783	    |	    104.2348	  |
|     8M(125M)     |       203.5195    |    203.0325 |	    204.6558	    |	    205.5065	  |
|    16M(256M)     |       401.1902    |    403.4908 |	    393.6258	    |	    421.6687	  |
|    32M(532M)     |       804.2793    |    789.9665 |	    773.4302	    |	    833.1732	  |
|    64M(1.1G)     |      1572.1955    |   1576.7877 |	   1542.4022	    |	   1607.2346	  |
|   128M(2.25G)    |      2985.2277    |   2980.7525 |	   2921.0990	    |	   3042.5842	  |

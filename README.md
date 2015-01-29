# healthCheck_cloudera_cluster

This script will check the health of the cloudera cluster. 

This will take two parameters:
    1. Name of the cluster
    2. Host of the Cloudera Manager

It will then perform the following actions: 

    1. Restart the Cloudera Cluster 
    
    2. Check the health of the following services 
    
      1. HDFS 
      
      2. YARN 
      
      3. ZOOKEEPER
      
    3. If the health of any of the services is not good it will then try to restart the service 3 times. 
    
    4. Upon failure to restart the service the script will exit.

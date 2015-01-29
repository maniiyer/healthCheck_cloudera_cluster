##################
#@author: maniiyer
##################
import time
import sys

#The cm_api is the cloud era manager api client that is used for accessing the instance.
#Refer wiki docs on how to install the cm_api client
from cm_api.api_client import ApiResource


#Setting the cloudera maanger host so that we can access the api

print "[logger.info] Enter the cluster name: " 
cluster_name = raw_input()
print "[logger.info] Setting the cluster name to: " + cluster_name

print "[logger.info] Please enter the host ip of the cloudera manager:"
cm_host = raw_input()
print "[logger.info] Setting the host ip address to: " + cm_host

#Authenticating the admin user
api = ApiResource(cm_host, username = "admin", password = "admin")

#Getting our cluster and assigning our cluster as a variable.
mycluster = api.get_cluster(cluster_name)
print "[logger.info] Used the api to retrieve the clustername: " + mycluster.name

print "[logger.info] Since we restarted the cloudera manager...we will be restarting our cluster"
print "[logger.info] Stopping the cluster..."
mycluster.stop().wait()
print "[logger.info] Stopped the cluster..."
time.sleep(30)
print "[logger.info] Starting the cluster..."
mycluster.start().wait()
time.sleep(60)
print "[logger.info] Started the cluster..."
#Assigning the services in the cluster to a variable
for service in mycluster.get_all_services():
    if service.name == "hdfs":
        hdfs = service
        elif service.name == "yarn":
            yarn = service
        elif service.name == "zookeeper":
            zookeeper = service

#Checking for the health of hdfs and if not good restarting it.

if hdfs.healthSummary != "GOOD":
    print "[logger.info] The returned health summary is not GOOD. we have the value: " + hdfs.healthSummary
        print "[logger.info] restarting the service"
        retry = 3
        print "[logger.info] We will try to restart the service 3 times"
        while retry != 0:
            print "[logger.info] Trying to restart the hdfs service"
                hdfs.restart().wait()
                print "[logger.info] We have sent our API command to restart the cluster"
                print "[logger.info] We will be sleeping for some seconds to give the cluster time to restart"
                time.sleep(60)
                status = mycluster.get_service("hdfs").healthSummary
                print "[logger.info] After restarting the cluster we have got the status of the cluster to be: " + status
                if status != "GOOD":
                    retry = retry - 1
                        if retry == 0:
                            print "[logger.info] Tried to restart the cluster 3 times...Seems like there is some real trouble"
                                print "[logger.info] Exiting the script and marking the build as failure"
                                sys.exit(1)
                else:
                    print "[logger.info] Have returned the cluster to Good Health"
                        retry = 0
else:
    print "[logger.info] The HDFS cluster is in good health moving along"

#Checking for the health of yarn and if not good restarting it.

if yarn.healthSummary != "GOOD":
    print "[logger.info] The returned health summary is not GOOD. we have the value: " + yarn.healthSummary
        print "[logger.info] restarting the yarn service"
        retry = 3
        print "[logger.info] We will try to restart the service 3 times"
        while retry != 0:
            print "[logger.info] Trying to restart the cluster"
                yarn.restart().wait()
                print "[logger.info] We have sent our API command to restart the cluster"
                print "[logger.info] We will be sleeping for some seconds to give the cluster time to restart"
                time.sleep(60)
                status = mycluster.get_service("yarn").healthSummary
                print "[logger.info] After restarting the cluster we have got the status of the cluster to be: " + status
                if status != "GOOD":
                    retry = retry - 1
                        if retry == 0:
                            print "[logger.info] Tried to restart the cluster 3 times...Seems like there is some real trouble"
                                print "[logger.info] Exiting the script and marking the build as failure"
                                sys.exit(1)
                else:
                    print "[logger.info] Have returned the cluster to Good Health"
                        retry = 0
else:
    print "[logger.info] The yarn cluster is in good health moving along"

#Checking for the health of zookeeper and if not good restarting it.

if zookeeper.healthSummary != "GOOD":
    print "[logger.info] The returned health summary is not GOOD. we have the value: " + zookeeper.healthSummary
        print "[logger.info] restarting the zookeeper service"
        retry = 3
        print "[logger.info] We will try to restart the service 3 times"
        while retry != 0:
            print "[logger.info] Trying to restart the cluster"
                zookeeper.restart().wait()
                print "[logger.info] We have sent our API command to restart the cluster"
                print "[logger.info] We will be sleeping for some seconds to give the cluster time to restart"
                time.sleep(60)
                status = mycluster.get_service("zookeeper").healthSummary
                print "[logger.info] After restarting the cluster we have got the status of the cluster to be: " + status
                if status != "GOOD":
                    retry = retry - 1
                        if retry == 0:
                            print "[logger.info] Tried to restart the cluster 3 times...Seems like there is some real trouble"
                                print "[logger.info] Exiting the script and marking the build as failure"
                                sys.exit(1)
                else:
                    print "[logger.info] Have returned the cluster to Good Health"
                        retry = 0
else:
    print "[logger.info] The zookeper cluster is in good health moving along"


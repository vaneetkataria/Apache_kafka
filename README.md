#Running the multibroker kafka cluser .
1. sudo docker-compose -f multibrokercluster.yml up/down 
2. Put host entries on local 127.0.0.1 kafak1 kafka2 kafak3 
3. Inside docker hostnames will be resolved automatically as container names are the same 
   and on local machine hostnames will be resolved by host entries. 
4. If this kafka broker has to be accessed from a remote machine then put 
   (Public IP of this machine if always same OR any DNS host entry which can route to 
    any node of this kafka)
    

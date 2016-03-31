# Dynamic Cluster Change
##Problem
You want you clients to automatically switch over to a secondary cluster when the primary cluster fails.
##Solution
The solution is to intercept the calls to the client, see if the current client is connected to the cluster and if it is not then attempt to re-connect to the known clusters
##Getting the code
The code for this example is in Github: https://github.com/aerospike/dynamic-cluster-change.git

Clone the GitHub repository using the command:
```
git clone https://github.com/aerospike/dynamic-cluster-change.git
```

using System;
using Aerospike.Client;
using System.Collections.Generic;

namespace ClusterChange
{
	public class DynamicAerospikeClient
	{
		private AerospikeClient client;
		private Cluster[] clusters;
		private Cluster connectedCluster = null;

		public DynamicAerospikeClient (params Cluster []clusters) 
		{
			this.clusters = clusters;
			this.Connect ();
		}

		#region Cluster reconnection logic
		/*
		 * If there is no connection (the client is null) then iterate through the list of clients and connect to
		 * the first one we can. If the name of a cluster is specified then connect only to that cluster.
		 */
		private void Connect(string clusterName = null) {
			AerospikeException lastException = null;
			if (client != null) {
				throw new InvalidOperationException ("client should be null");
			}

			// TODO: This should be in a critical section so that only one thread can attempt the reconnect at a time.
			// acquireLock();
			// if (this.client == null) {  	// In case another thread created a new client whilst we were waiting.
			foreach (Cluster cluster in clusters) {
				if (clusterName == null || clusterName.Equals (cluster.Name)) {
					// We must force the clusters to fail if they cannot connect so we will automatcially move onto the next cluster
					cluster.Policy.failIfNotConnected = true;

					try {
						AerospikeClient newClient = new AerospikeClient (cluster.Policy, cluster.Hosts);
						this.client = newClient;
						connectedCluster = cluster;
						Console.WriteLine ("Connected to cluster: " + cluster.Name);
						return;
					} catch (AerospikeException ae) {
						// We could not connect to this cluster, log it and move onto the next one.
						Console.Error.WriteLine ("Could not connect to cluster " + cluster.Name + ", trying next one. " + ae.Message);
						lastException = ae;
					}
				}
			}
			// To get here, we could not find a cluster to connect to. 
			if (lastException != null) {
				throw lastException;
			}
			else {					
				throw new AerospikeException (ResultCode.ILLEGAL_STATE);

			}
			// TODO: End critical section
			// releaseLock();  (Must be done in a finally block)
		}

		private void Disconnect() {
			if (this.client != null) {
				client.Close ();
				this.client = null;
				this.connectedCluster = null;
			}
		}

		private AerospikeClient GetClient() {
			if (client == null) {
				Connect ();
			} else if (!client.Connected && clusters.Length > 1) {
				// Throw away this client and get a new one, unless there's only 1 cluster specified
				// TODO: wrapper this in the same critical section as above: (must be a reenterant lock)
				// acquireLock();
				Disconnect ();
				Connect ();
				// TODO: end critical section
				// releaseLock();
			}
			return client;
		}

		public void SwitchCluster(string newCluster) {
			bool found = false;
			if (newCluster == null) {
				throw new InvalidOperationException("cluster name must be specified");
			}
			if (connectedCluster != null && connectedCluster.Name.Equals (newCluster)) {
				// Already connected to this cluster, so do nothing
				return;
			}
			// Make sure we know about this cluster
			foreach (Cluster cluster in clusters) {
				if (newCluster.Equals (cluster.Name)) {
					found = true;
					break;
				}
			}
			if (!found) {
				throw new InvalidOperationException("Cluster " + newCluster + " is not known");
			}
			// TODO: wrapper this in the same critical section as above: (must be a reenterant lock)
			// acquireLock();
			Disconnect ();
			Connect (newCluster);
			// TODO: end critical section
			// releaseLock();
		}
		#endregion

		#region Aerospike client interface
		public Record Get(Policy policy, Key key) {
			return GetClient ().Get (policy, key);
		}

		public bool Connected 
		{
			get { return GetClient ().Connected; }
		}
		public bool Exists (Policy policy, Key key)
		{
			return GetClient().Exists(policy, key);
		}
		public Record[] Get (BatchPolicy policy, Key[] keys, params string[] binNames)
		{
			return GetClient ().Get (policy, keys, binNames);
		}
		public Record[] Get (BatchPolicy policy, Key[] keys) {
			return GetClient().Get(policy, keys);
		}
		public void Get (BatchPolicy policy, List<BatchRead> records)
		{
			GetClient ().Get (policy, records);
		}
		public Record Get (Policy policy, Key key, params string[] binNames)
		{
			return GetClient ().Get (policy, key, binNames);
		}
		#endregion
	}
}


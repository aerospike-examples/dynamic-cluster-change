using System;
using Aerospike.Client;
using System.Threading;

namespace ClusterChange
{
	public class Program
	{
		public void Execute() {
			
			ClientPolicy policy1 = new ClientPolicy ();
			Host host1 = new Host ("172.28.128.3", 3000);
			Cluster cluster1 = new Cluster (policy1, "cluster1", host1);

			ClientPolicy policy2 = new ClientPolicy ();
			Host host2 = new Host ("172.28.128.4", 3000);
			Cluster cluster2 = new Cluster (policy2, "cluster2", host2);

			DynamicAerospikeClient client = new DynamicAerospikeClient (cluster1, cluster2);

			client.SwitchCluster ("cluster2");		// Just for fun, start on the second cluster

			Key key = new Key ("test", "testSet", "key1");

			while (true) {
				try {
					bool connected = client.Connected;
					Console.WriteLine("Client connected: " + connected);
					if (connected) {
						Record r = client.Get(null, key);
						Console.WriteLine("Read: " + r.GetString("name"));
					}
				}
				catch (AerospikeException ae) {
					Console.WriteLine("Received exception " + ae.Message);
				}
				Thread.Sleep(1000);
			}
		}

		static void Main(string[] args) {
			new Program ().Execute ();
		}
	}
}


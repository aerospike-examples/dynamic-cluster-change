using System;
using Aerospike.Client;

namespace ClusterChange
{
	/// <summary>
	/// Cluster. This class encapsulates all the information we know about a cluster -- it's name
	/// (should be unique across all clusters), the ClientPolicy and the set of Hosts which are
	/// in this cluster.
	/// 
	/// Each cluster can have it's own Client Policy which allows different security credentials to
	/// be specified for security-enabled enterprises.
	/// </summary>
	public class Cluster
	{
		public string Name { get; }
		public Host[] Hosts { get; }
		public ClientPolicy Policy { get; }
		public Cluster (ClientPolicy policy, string name, params Host[] hosts)
		{
			this.Policy = policy;
			this.Name = name;
			this.Hosts = hosts;
		}
	}
}


using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using Common;
using Common.Patterns;
using System.Threading.Tasks;
using Common.Patterns.Tasks;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticLibrary
{
	public class Cluster : IEquatable<Cluster>, IEquatable<string>
	{
		public static readonly Cluster MasterCluster = null;
        public static ConcurrentBag<Cluster> Clusters = new ConcurrentBag<Cluster>();
		private static CTS.List<INode> UnAssociatedNodes = new CTS.List<INode>();

		static Cluster()
		{
            MasterCluster = new Cluster("**LogicalMaster**") { IsMaster = true };
		}

        public sealed class OpsCenterInfo
        {
            public Version Version;
            public bool? RepairServiceEnabled;
        }

        public Cluster()
		{
			Clusters.Add(this);
            this.OpsCenter = new OpsCenterInfo();
		}

		public Cluster(string clusterName)
			: this()
		{
			this.Name = clusterName;
		}

		public string Name { get; private set; }

        public bool IsMaster { get; private set; }

		private CTS.List<IDataCenter> _dataCenters = new CTS.List<IDataCenter>();
		public IEnumerable<IDataCenter> DataCenters { get { return this._dataCenters; } }

		private List<IEvent> _events = new List<IEvent>();
		public IEnumerable<IEvent> Events { get { lock (this._events) { return this._events.ToArray(); } } }

        private CTS.List<IKeyspace> _keySpaces = new CTS.List<IKeyspace>();
        public IEnumerable<IKeyspace> Keyspaces { get { return this._keySpaces; } }

        public IEnumerable<IKeyspace> GetKeyspaces(IDataCenter datacenter)
        {
            return this._keySpaces.Where(k => k.DataCenters.Contains(datacenter));
        }

        public IEnumerable<IKeyspace> GetKeyspaces(string keyspaceName)
        {
            return this._keySpaces.Where(k => k.Name == keyspaceName);
        }

        public OpsCenterInfo OpsCenter { get; private set; }

		public IEvent AssociateItem(IEvent eventItem)
		{

			return eventItem;
		}

        public IKeyspace AssociateItem(IKeyspace ksItem)
        {
            this._keySpaces.Lock();
            try
            {
                var existingKS = this._keySpaces.FirstOrDefault(ks => ks.Equals(ksItem));

                if (existingKS == null)
                {
                    this._keySpaces.Add(ksItem);
                }
                else
                {
                    ksItem = existingKS;
                }
            }
            finally
            {
                this._keySpaces.UnLock();
            }

            return ksItem;
        }

        public IDataCenter TryGetDataCenter(string dataCenterName)
        {
           return this._dataCenters.FirstOrDefault(d => d.Name == dataCenterName);
        }

        public object ToDump()
        {
            return new { Name = this.Name, DataCenters = this.DataCenters, Keyspaces = this.Keyspaces };
        }

        #region Static Methods

        public static Cluster GetCurrentOrMaster()
        {
            if(Clusters.Count == 1)
            {
                return Clusters.First();
            }

            return MasterCluster;
        }

        public static Cluster TryGetAddCluster(string clusterName)
		{
			var cluster = Clusters.FirstOrDefault(c => c.Name == clusterName);

			if (cluster == null)
			{
				cluster = new Cluster(clusterName);
			}

			return cluster;
		}

        public static Cluster TryGetCluster(string clusterName)
        {
            return Clusters.FirstOrDefault(c => c.Name == clusterName);
        }

        public static IDataCenter TryGetAddDataCenter(string dataCenterName, Cluster cluster = null)
		{
			var currentCuster = cluster == null ? MasterCluster : cluster;
			var currentDC = currentCuster._dataCenters.FirstOrDefault(d => d.Name == dataCenterName);

			if (currentDC == null)
			{
				currentDC = new DataCenter(currentCuster, dataCenterName);
				currentCuster._dataCenters.Add(currentDC);
			}

			return currentDC;
		}
		public static IDataCenter TryGetAddDataCenter(string dataCenterName, string clusterName = null)
		{
			var currentCuster = string.IsNullOrEmpty(clusterName) ? MasterCluster : TryGetAddCluster(clusterName);

			return TryGetAddDataCenter(dataCenterName, currentCuster);
		}

        public static IDataCenter TryGetDataCenter(string dataCenterName, Cluster cluster)
        {
            if (string.IsNullOrEmpty(dataCenterName)) return null;

            var currentCuster = cluster == null ? MasterCluster : cluster;

            return currentCuster.TryGetDataCenter(dataCenterName);
        }
        public static IDataCenter TryGetDataCenter(string dataCenterName, string clusterName = null)
        {
            if (string.IsNullOrEmpty(dataCenterName)) return null;

            var currentCuster = string.IsNullOrEmpty(clusterName) ? MasterCluster : TryGetCluster(clusterName);

            return TryGetDataCenter(dataCenterName, currentCuster);
        }

        public static INode TryGetAddNode(string nodeId, string dataCenterName = null, string clusterName = null)
		{
			if (!NodeIdentifier.ValidNodeIdName(nodeId))
			{ return null; }

			var currentDC = (DataCenter)((string.IsNullOrEmpty(dataCenterName) ? null : TryGetAddDataCenter(dataCenterName, clusterName)));

			if (currentDC == null)
			{
				var node = UnAssociatedNodes.FirstOrDefault(n => n.Equals(nodeId));

				if (node == null)
				{
					var cluster = string.IsNullOrEmpty(clusterName) ? MasterCluster : TryGetAddCluster(clusterName);

                    node = cluster.DataCenters.SelectMany(dc => dc.Nodes).FirstOrDefault(n => n.Equals(nodeId));

                    if (node == null)
                    {
                        node = new Node(cluster, nodeId);
                        UnAssociatedNodes.Add(node);
                    }
                }

				return node;
			}

			return currentDC.TryGetAddNode(nodeId);
		}

        public static INode TryGetAddNode(string nodeId, IDataCenter dataCenter)
        {
            if (!NodeIdentifier.ValidNodeIdName(nodeId))
            { return null; }

            return ((DataCenter)dataCenter)?.TryGetAddNode(nodeId);
        }

        public static INode TryGetAddNode(NodeIdentifier nodeId, string dataCenterName = null, string clusterName = null)
		{
			if (nodeId == null)
			{ return null; }

			var currentDC = (DataCenter)((string.IsNullOrEmpty(dataCenterName) ? null : TryGetAddDataCenter(dataCenterName, clusterName)));

			if (currentDC == null)
			{
				var node = UnAssociatedNodes.FirstOrDefault(n => n.Id == nodeId);

				if (node == null)
				{
					var cluster = string.IsNullOrEmpty(clusterName) ? MasterCluster : TryGetAddCluster(clusterName);

                    node = cluster.DataCenters.SelectMany(dc => dc.Nodes).FirstOrDefault(n => n.Id == nodeId);

                    if (node == null)
                    {
                        node = new Node(cluster, nodeId);
                        UnAssociatedNodes.Add(node);
                    }
				}

				return node;
			}

			return currentDC.TryGetAddNode(nodeId);
		}

        public static INode TryGetNode(string nodeId, string dataCenterName = null, string clusterName = null, bool includeUnAssociatedNodes = true)
        {
            var dcNodes = GetNodes(dataCenterName, clusterName, includeUnAssociatedNodes);

            if(dcNodes != null)
            {
                return dcNodes.FirstOrDefault(n => n.Id.Equals(nodeId));
            }

            return null;
        }

        public static IEnumerable<INode> GetNodes(string dataCenterName = null, string clusterName = null, bool includeUnAssociatedNodes = true)
        {
            if(!string.IsNullOrEmpty(dataCenterName))
            {
                var dcInstance = TryGetDataCenter(dataCenterName, clusterName);
                return dcInstance == null ? Enumerable.Empty<INode>() : dcInstance.Nodes;
            }

            var cluster = TryGetCluster(clusterName);
            var clusterNodes = cluster?.DataCenters?.SelectMany(dc => dc.Nodes);

            if(includeUnAssociatedNodes)
            {
                if(clusterNodes == null)
                {
                    return UnAssociatedNodes.ToList();
                }

                var listNodes = clusterNodes.ToList();

                listNodes.AddRange(UnAssociatedNodes);

                return listNodes;
            }

            return clusterNodes == null ? Enumerable.Empty<INode>() : clusterNodes;
        }

		public static INode AssociateDataCenterToNode(string dataCenterName, string nodeId, string clusterName = null)
		{
			if (string.IsNullOrEmpty(nodeId))
			{ throw new ArgumentNullException("nodeId cannot be null"); }
			if(string.IsNullOrEmpty(dataCenterName))
			{ throw new ArgumentNullException("dataCenter cannot be null"); }

			var currentDC = (DataCenter) TryGetAddDataCenter(dataCenterName, clusterName);
			var node = (Node) UnAssociatedNodes.FirstOrDefault(n => n.Equals(nodeId));

			if(node == null)
			{
				return currentDC.TryGetAddNode(nodeId);
			}

			UnAssociatedNodes.Remove(node);
			return currentDC.TryGetAddNode(node);
		}

        public static INode AssociateDataCenterToNode(string dataCenterName, INode node)
        {
            if (node == null)
            { throw new ArgumentNullException("node cannot be null"); }
            if (string.IsNullOrEmpty(dataCenterName))
            { throw new ArgumentNullException("dataCenter cannot be null"); }

            var currentDC = (DataCenter)TryGetAddDataCenter(dataCenterName, node.Cluster);
            var removeAssocNode = (Node)UnAssociatedNodes.FirstOrDefault(n => n.Equals(node));

            if (currentDC != null && removeAssocNode != null)
            {
                UnAssociatedNodes.Remove(node);
            }

            return currentDC?.TryGetAddNode(node);
        }

        /// <summary>
        /// This will either name the associated cluster (if name is null) or create a new cluster and associate this node and it&apos;s associated DC to the new cluster
        /// </summary>
        /// <param name="node"></param>
        /// <param name="clusterName"></param>
        /// <returns></returns>
        public static Cluster NameClusterAssociatewItem(INode node, string clusterName)
        {
            if(node != null && !string.IsNullOrEmpty(clusterName) && node.DataCenter == null)
            {
                var cluster = TryGetAddCluster(clusterName);

                lock (node)
                {
                    if (cluster != null && node.Cluster.Name != clusterName && node.DataCenter == null)
                    {
                        ((Node)node).SetCluster(cluster);
                        return cluster;
                    }
                }
            }

            return NameClusterAssociatewItem(node?.DataCenter, clusterName);
        }

        /// <summary>
        /// This will either name the associated cluster (if name is null) or create a new cluster and associated DC to the new cluster
        /// </summary>
        /// <param name="node"></param>
        /// <param name="clusterName"></param>
        /// <returns></returns>
        public static Cluster NameClusterAssociatewItem(IDataCenter dc, string clusterName)
        {
            if (dc == null || string.IsNullOrEmpty(clusterName)) return null;

            var cluster = TryGetAddCluster(clusterName);

            lock (dc)
            {
                if (cluster != null && dc.Cluster.Name != clusterName)
                {
                    var oldCluster = dc.Cluster;

                    if (!cluster._dataCenters.Contains(dc))
                    {
                        cluster._dataCenters.Add(dc);
                    }

                    ((DataCenter)dc).AssociateItem(cluster);

                    if (!oldCluster.IsMaster)
                    {
                        oldCluster._dataCenters.Remove(dc);
                    }
                }
            }

            return cluster;
        }

        #endregion

		#region IEquatable
		public bool Equals(Cluster other)
		{
			if (ReferenceEquals(this, other))
			{ return true; }

			return this.Name == other.Name;
		}

		public bool Equals(string other)
		{
			return this.Name == other;
		}
		#endregion

		#region Object Overrides
		public override string ToString()
		{
			return string.Format("Cluster{{Name=\"{0}\", DataCenters={{{1}}}}}. Keyspaces={{{2}}}",
									this.Name,
									this.DataCenters == null
										? string.Empty
										: string.Join(",", this.DataCenters.Select(i => i.Name)),
                                    string.Join(",", this.Keyspaces.Select(i => i.Name)));
		}

		public override bool Equals(object obj)
		{
			if(obj is Cluster)
			{
				return this.Equals((Cluster)obj);
			}
			if(obj is string)
			{
				return this.Equals((string)obj);
			}
			return false;
		}

		public override int GetHashCode()
		{
			return string.IsNullOrEmpty(this.Name) ? 0 : this.Name.GetHashCode();
		}

		#endregion
	}
}

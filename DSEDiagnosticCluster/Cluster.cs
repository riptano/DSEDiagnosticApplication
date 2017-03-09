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
		public static Cluster CurrentCluster = null;
        public static ConcurrentBag<Cluster> Clusters = new ConcurrentBag<Cluster>();
		private static CTS.List<INode> UnAssociatedNodes = new CTS.List<INode>();
		private static QueueProcessor<Tuple<Cluster, IEvent>> EventProcessQueue = new QueueProcessor<Tuple<Cluster, IEvent>>();

		static Cluster()
		{
			EventProcessQueue.OnProcessMessageEvent += EventProcessQueue_OnProcessMessageEvent;
			EventProcessQueue.Name = "Cluster-IEvent";
			EventProcessQueue.StartQueue(false);
            CurrentCluster = new Cluster();
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
		private ConcurrentBag<IDataCenter> _dataCenters = new ConcurrentBag<IDataCenter>();
		public IEnumerable<IDataCenter> DataCenters { get { return this._dataCenters; } }

		private List<IEvent> _events = new List<IEvent>();
		public IEnumerable<IEvent> Events { get { lock (this._events) { return this._events.ToArray(); } } }

        public OpsCenterInfo OpsCenter { get; private set; }

		public IEvent AssociateItem(IEvent eventItem)
		{
			if (eventItem != null)
			{
				EventProcessQueue.Enqueue(new Tuple<Cluster, IEvent>(this, eventItem));
			}

			return eventItem;
		}

		#region Static Methods

		public static Cluster TryGetAddCluster(string clusterName, bool makeCurrent = true)
		{
			var isCurrent = CurrentCluster?.Equals(clusterName);

			if (isCurrent.HasValue && isCurrent.Value)
			{
				return CurrentCluster;
			}

			var cluster = Clusters.FirstOrDefault(c => c.Name == clusterName);

			if (cluster == null)
			{
				cluster = new Cluster(clusterName);
			}

			if (makeCurrent)
			{
                CurrentCluster = cluster;
            }

			return cluster;
		}

        public static Cluster TryGetCluster(string clusterName)
        {
            var isCurrent = CurrentCluster?.Equals(clusterName);

            if (isCurrent.HasValue && isCurrent.Value)
            {
                return CurrentCluster;
            }

            return Clusters.FirstOrDefault(c => c.Name == clusterName);
        }

        public static IDataCenter TryGetAddDataCenter(string dataCenterName, Cluster cluster = null)
		{
			var currentCuster = cluster == null ? CurrentCluster : cluster;
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
			var currentCuster = string.IsNullOrEmpty(clusterName) ? CurrentCluster : TryGetAddCluster(clusterName);

			return TryGetAddDataCenter(dataCenterName, currentCuster);
		}

        public static IDataCenter TryGetDataCenter(string dataCenterName, Cluster cluster)
        {
            if (string.IsNullOrEmpty(dataCenterName)) return null;

            var currentCuster = cluster == null ? CurrentCluster : cluster;
            var currentDC = currentCuster._dataCenters.FirstOrDefault(d => d.Name == dataCenterName);

            return currentDC;
        }
        public static IDataCenter TryGetDataCenter(string dataCenterName, string clusterName = null)
        {
            if (string.IsNullOrEmpty(dataCenterName)) return null;

            var currentCuster = string.IsNullOrEmpty(clusterName) ? CurrentCluster : TryGetCluster(clusterName);

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
					var cluster = string.IsNullOrEmpty(clusterName) ? CurrentCluster : TryGetAddCluster(clusterName);

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
					var cluster = string.IsNullOrEmpty(clusterName) ? CurrentCluster : TryGetAddCluster(clusterName);

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

        public static IEnumerable<INode> GetNodes(string dataCenterName = null, string clusterName = null, bool includeUnAssociatedNodes = true)
        {
            if(!string.IsNullOrEmpty(dataCenterName))
            {
                var dcInstance = TryGetDataCenter(dataCenterName, clusterName);
                return dcInstance == null ? Enumerable.Empty<INode>() : dcInstance.Nodes;
            }

            var cluster = TryGetCluster(clusterName);
            var clusterNodes = cluster.DataCenters?.SelectMany(dc => dc.Nodes);

            if(clusterNodes != null && includeUnAssociatedNodes)
            {
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

        /// <summary>
        /// This will either name the associated cluster (if name is null) or create a new cluster and associate this node and it&apos;s associated DC to the new cluster
        /// </summary>
        /// <param name="node"></param>
        /// <param name="clusterName"></param>
        /// <param name="alwaysCreateNewCluster"></param>
        /// <returns></returns>
        public static Cluster NameClusterAssociatewItem(INode node, string clusterName, bool alwaysCreateNewCluster = false)
        {
            if(node != null && !string.IsNullOrEmpty(clusterName) && node.DataCenter == null)
            {
                if (!alwaysCreateNewCluster && string.IsNullOrEmpty(node.Cluster.Name))
                {
                    lock (node.Cluster)
                    lock(node)
                    {
                        if (string.IsNullOrEmpty(node.Cluster.Name) && node.DataCenter == null)
                        {
                            node.Cluster.Name = clusterName;
                            return node.Cluster;
                        }

                        if (node.Cluster.Name == clusterName)
                        {
                            return node.Cluster;
                        }

                        if(node.DataCenter != null)
                        {
                            return NameClusterAssociatewItem(node.DataCenter, clusterName, alwaysCreateNewCluster);
                        }
                    }
                }

                var cluster = TryGetAddCluster(clusterName, false);

                if (cluster != null && node.Cluster.Name != clusterName)
                {
                    ((Node)node).SetCluster(cluster);
                }

                return cluster;
            }

            return NameClusterAssociatewItem(node?.DataCenter, clusterName, alwaysCreateNewCluster);
        }

        /// <summary>
        /// This will either name the associated cluster (if name is null) or create a new cluster and associated DC to the new cluster
        /// </summary>
        /// <param name="node"></param>
        /// <param name="clusterName"></param>
        /// <param name="alwaysCreateNewCluster"></param>
        /// <returns></returns>
        public static Cluster NameClusterAssociatewItem(IDataCenter dc, string clusterName, bool alwaysCreateNewCluster = false)
        {
            if (dc == null || string.IsNullOrEmpty(clusterName)) return null;

            if (!alwaysCreateNewCluster && string.IsNullOrEmpty(dc.Cluster.Name))
            {
                lock (dc.Cluster)
                lock (dc)
                {
                    if (string.IsNullOrEmpty(dc.Cluster.Name))
                    {
                        dc.Cluster.Name = clusterName;
                        return dc.Cluster;
                    }

                    if (dc.Cluster.Name == clusterName)
                    {
                        return dc.Cluster;
                    }
                }
            }

            var cluster = TryGetAddCluster(clusterName, false);

            if (cluster != null && dc.Cluster.Name != clusterName)
            {
                ((DataCenter)dc).AssociateItem(cluster);
            }

            return cluster;
        }

        #endregion

        #region Processing Queue

        private static void EventProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<Cluster, IEvent>> sender, QueueProcessor<Tuple<Cluster, IEvent>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._events)
			{
				processMessageArgs.ProcessedMessage.Item1._events.Add(processMessageArgs.ProcessedMessage.Item2);
			}
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
			return string.Format("Cluster{Name=\"{0}\", DataCenters={{{1}}}}",
									this.Name,
									this.DataCenters == null
										? string.Empty
										: string.Join(",", this.DataCenters.Select(i => i.Name)));
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

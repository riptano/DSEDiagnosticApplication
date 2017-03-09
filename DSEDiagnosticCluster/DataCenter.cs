using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using Common.Patterns;
using System.Threading.Tasks;
using Common.Patterns.Tasks;

namespace DSEDiagnosticLibrary
{
	public interface IDataCenter : IEquatable<IDataCenter>, IEquatable<string>
	{
		Cluster Cluster { get; }
		string Name { get; }
		IEnumerable<INode> Nodes { get; }
		IEnumerable<LocalKeyspaceInfo> Keyspaces { get; }

		INode TryGetNode(string nodeId);
        INode TryGetAddNode(INode node);

        IEnumerable<IEvent> Events { get; }
		IEnumerable<IConfigurationLine> Configurations { get; }
		IEnumerable<IDDL> DDLs { get; }
	}

	internal sealed class DataCenter : IDataCenter
	{
		private static QueueProcessor<Tuple<DataCenter, IEvent>> EventProcessQueue = new QueueProcessor<Tuple<DataCenter, IEvent>>();
		private static QueueProcessor<Tuple<DataCenter, IConfigurationLine>> ConfigProcessQueue = new QueueProcessor<Tuple<DataCenter, IConfigurationLine>>();
		private static QueueProcessor<Tuple<DataCenter, IDDL>> DDLProcessQueue = new QueueProcessor<Tuple<DataCenter, IDDL>>();

		static DataCenter()
		{
			EventProcessQueue.OnProcessMessageEvent += EventProcessQueue_OnProcessMessageEvent;
			EventProcessQueue.Name = "DataCenter-IEvent";
			ConfigProcessQueue.OnProcessMessageEvent += ConfigProcessQueue_OnProcessMessageEvent;
			ConfigProcessQueue.Name = "DataCenter-IConfiguration";
			DDLProcessQueue.OnProcessMessageEvent += DDLProcessQueue_OnProcessMessageEvent;
			DDLProcessQueue.Name = "DataCenter-IDDL";
			DDLProcessQueue.StartQueue(false);
			ConfigProcessQueue.StartQueue(false);
			EventProcessQueue.StartQueue(false);
		}

		public DataCenter()
		{ }

		public DataCenter(Cluster cluster, string name)
		{
			this.Cluster = cluster;
			this.Name = name;
		}

		public INode TryGetAddNode(string nodeId)
		{
			if(!NodeIdentifier.ValidNodeIdName(nodeId))
			{ return null; }

			var node = this._nodes.FirstOrDefault(n => n.Equals(nodeId));

			if(node == null)
			{
				node = new Node(this, nodeId);
				this._nodes.Add(node);
			}

			return node;
		}

		public INode TryGetAddNode(NodeIdentifier nodeId)
		{
			if (nodeId == null)
			{ return null; }

			var node = this._nodes.FirstOrDefault(n => n.Equals(nodeId));

			if (node == null)
			{
				node = new Node(this, nodeId);
				this._nodes.Add(node);
			}

			return node;
		}

		public INode TryGetAddNode(INode node)
		{
			if (node == null)
			{ return null; }

			if(this._nodes.Contains(node))
			{
				node = this._nodes.First(n => n.Equals(node));
			}
			else
			{
				this._nodes.Add(node);
			}

			if (node.DataCenter == null)
			{
				((Node)node).SetNodeToDataCenter(this);
			}

			return node;
		}

		public IDataCenter AssociateItem(IEvent eventItem)
		{
			if (eventItem != null)
			{
				EventProcessQueue.Enqueue(new Tuple<DataCenter, IEvent>(this, eventItem));
				this.Cluster.AssociateItem(eventItem);
			}

			return this;
		}
		public IDataCenter AssociateItem(IConfigurationLine configItem)
		{
			if (configItem != null)
			{
				ConfigProcessQueue.Enqueue(new Tuple<DataCenter, IConfigurationLine>(this, configItem));
			}

			return this;
		}
		public IDataCenter AssociateItem(IDDL ddlItem)
		{
			if (ddlItem != null)
			{
				DDLProcessQueue.Enqueue(new Tuple<DataCenter, IDDL>(this, ddlItem));
			}

			return this;
		}

        /// <summary>
        /// This will override an existing cluster association without warning!
        /// </summary>
        /// <param name="newCluster"></param>
        /// <returns></returns>
        internal IDataCenter AssociateItem(Cluster newCluster)
        {
            if (newCluster == null) throw new NullReferenceException("newCluster cannot be null");

            if (!ReferenceEquals(this.Cluster, newCluster))
            {
                lock (this)
                {
                    if (!ReferenceEquals(this.Cluster, newCluster))
                    {                       
                        this.Cluster = newCluster;
                    }
                }
            }

            return this;
        }

        #region IDataCenter
        public Cluster Cluster
		{
			get;
			private set;
		}

		public IEnumerable<LocalKeyspaceInfo> Keyspaces
		{
			get
			{
				throw new NotImplementedException();
			}
		}

		public string Name
		{
			get;
			private set;
		}

		private ConcurrentBag<INode> _nodes = new ConcurrentBag<INode>();
		public IEnumerable<INode> Nodes
		{
			get { return this._nodes; }
		}

		public INode TryGetNode(string nodeId)
		{
			if (string.IsNullOrEmpty(nodeId))
			{ return null; }

			return this._nodes.FirstOrDefault(n => n.Equals(nodeId));
		}

		private List<IEvent> _events = new List<IEvent>();
		public IEnumerable<IEvent> Events { get { lock (this._events) { return this._events.ToArray(); } } }

		private List<IConfigurationLine> _configurations = new List<IConfigurationLine>();
		public IEnumerable<IConfigurationLine> Configurations { get { lock (this._configurations) { return this._configurations.ToArray(); } } }

		private List<IDDL> _ddls = new List<IDDL>();
		public IEnumerable<IDDL> DDLs { get { lock (this._ddls) { return this._ddls.ToArray(); } } }

		#endregion

		#region IEquatable
		public bool Equals(string other)
		{
			return this.Name == other;
		}

		public bool Equals(IDataCenter other)
		{
			return other == null ? false : this.Name == other.Name;
		}
		#endregion

		#region overrides

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(this, obj))
			{ return true; }
			if(obj is IDataCenter)
			{ return this.Equals((IDataCenter)obj); }
			if(obj is string)
			{ return this.Equals((string) obj);}

			return false;
		}

		public override int GetHashCode()
		{
			return this.Name == null ? 0 : this.Name.GetHashCode();
		}

		public override string ToString()
		{
			return string.Format("DataCenter{{{0}}}", string.IsNullOrEmpty(this.Name) ? "<Unknown>" : this.Name);
		}

        public static bool operator ==(DataCenter a, DataCenter b)
        {
            return ReferenceEquals(a, b) || (!ReferenceEquals(a, null) && a.Equals(b));
        }

        public static bool operator !=(DataCenter a, DataCenter b)
        {
            if (ReferenceEquals(a, null) && !ReferenceEquals(b, null)) return true;
            if (ReferenceEquals(b, null) && !ReferenceEquals(a, null)) return true;
            if (ReferenceEquals(a, b)) return false;

            return !a.Equals(b);
        }
        #endregion

        #region Processing Queue

        private static void EventProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<DataCenter, IEvent>> sender, QueueProcessor<Tuple<DataCenter, IEvent>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._events)
			{
				processMessageArgs.ProcessedMessage.Item1._events.Add(processMessageArgs.ProcessedMessage.Item2);
			}
		}

		private static void ConfigProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<DataCenter, IConfigurationLine>> sender, QueueProcessor<Tuple<DataCenter, IConfigurationLine>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._configurations)
			{
				processMessageArgs.ProcessedMessage.Item1._configurations.Add(processMessageArgs.ProcessedMessage.Item2);
			}
		}

		private static void DDLProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<DataCenter, IDDL>> sender, QueueProcessor<Tuple<DataCenter, IDDL>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._ddls)
			{
				processMessageArgs.ProcessedMessage.Item1._ddls.Add(processMessageArgs.ProcessedMessage.Item2);
			}
		}

		#endregion
	}
}

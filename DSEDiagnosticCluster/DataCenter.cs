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
        IEnumerable<IConfigurationLine> GetConfigurations(INode node);

        IEnumerable<IDDL> DDLs { get; }
	}

	internal sealed class DataCenter : IDataCenter
	{	
		static DataCenter()
		{			
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
			}

			return this;
		}
		public IDataCenter AssociateItem(IConfigurationLine configItem)
		{
            lock(this._configurations)
            {
                this._configurations.Add(configItem);
            }
			
			return this;
		}
		public IDataCenter AssociateItem(IDDL ddlItem)
		{
			
			return this;
		}

        public IConfigurationLine ConfigurationMatch(string property,
                                                        string value,
                                                        ConfigTypes type,
                                                        SourceTypes source)
        {
            lock(this._configurations)
            {
                return this._configurations.FirstOrDefault(c => c.Match(this, property, value, type, source));
            }
        }
        
        public IEnumerable<IConfigurationLine> GetConfigurations(INode node)
        {
            lock (this._configurations) { return this._configurations.Where(c => ((YamlConfigurationLine)c).CommonNodes.Any(n => n == node)); }
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

	}
}

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

        IConfigurationLine ConfigurationMatch(string property, string value, ConfigTypes type, SourceTypes source);

        IEnumerable<INode> Nodes { get; }
		IEnumerable<LocalKeyspaceInfo> Keyspaces { get; }

		INode TryGetNode(string nodeId);
        INode TryGetAddNode(INode node);

        IEnumerable<IEvent> Events { get; }
		IEnumerable<IConfigurationLine> Configurations { get; }
        IEnumerable<IConfigurationLine> GetConfigurations(INode node);

        IEnumerable<IDDL> DDLs { get; }
	}

    internal class PlaceholderDataCenter : IDataCenter
    {
        protected PlaceholderDataCenter()
        { }

        internal PlaceholderDataCenter(INode node, string name, string className)
        {
            this._name = name;
            this._className = className;
            this._nodes.Add(node);
        }

        internal INode AddNode(INode node)
        {
            if (node == null)
            { return null; }

            if (!this._nodes.Contains(node))
            {
                this._nodes.Add(node);
            }

            return node;
        }

        virtual public IConfigurationLine ConfigurationMatch(string property,
                                                                string value,
                                                                ConfigTypes type,
                                                                SourceTypes source)
        {
            return ((DataCenter)this.Nodes.First().DataCenter).ConfigurationMatch(property, value, type, source);
        }

        #region IDataCenter
        virtual public Cluster Cluster
        {
            get { return this._nodes.First().Cluster; }
            protected set { throw new NotImplementedException(); }
        }

        virtual public IEnumerable<LocalKeyspaceInfo> Keyspaces
        {
            get
            {
                return Enumerable.Empty<LocalKeyspaceInfo>();
            }
        }

        private string _name = null;
        private string _className = null;
        virtual public string Name
        {
            get { return string.Format("{0}<{1}>", this._name, this._nodes.Count()); }
            protected set { throw new NotImplementedException(); }
        }

        protected ConcurrentBag<INode> _nodes = new ConcurrentBag<INode>();
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
        virtual public INode TryGetAddNode(INode node)
        {
            throw new NotImplementedException();
        }

        virtual public IEnumerable<IConfigurationLine> GetConfigurations(INode node)
        {
            throw new NotImplementedException();
        }

        virtual public IEnumerable<IEvent> Events { get { throw new NotImplementedException(); } }

        virtual public IEnumerable<IConfigurationLine> Configurations { get { throw new NotImplementedException(); } }

        virtual public IEnumerable<IDDL> DDLs { get { throw new NotImplementedException(); } }

        #endregion

        #region IEquatable
        virtual public bool Equals(string other)
        {
            return this._name == other;
        }

        virtual public bool Equals(IDataCenter other)
        {
            if (other is DataCenter)
            {
                return other == null ? false : this._name == other.Name;
            }

            return other == null ? false : this._name == ((PlaceholderDataCenter)other)._name;
        }
        #endregion

        #region overrides

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj))
            { return true; }
            if (obj is IDataCenter)
            { return this.Equals((IDataCenter)obj); }
            if (obj is string)
            { return this.Equals((string)obj); }

            return false;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("{0}{{{1}}}", this._className, this.Name);
        }

        #endregion

    }

    internal sealed class DataCenter : PlaceholderDataCenter
	{
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

		override public INode TryGetAddNode(INode node)
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


        override public IEnumerable<IConfigurationLine> GetConfigurations(INode node)
        {
            lock (this._configurations) { return this._configurations.Where(c => ((YamlConfigurationLine)c).Node.Equals(node)); }
        }

        override public IConfigurationLine ConfigurationMatch(string property,
                                                                string value,
                                                                ConfigTypes type,
                                                                SourceTypes source)
        {
            lock (this._configurations)
            {
                return this._configurations.FirstOrDefault(c => c.Match(this, property, value, type, source));
            }
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
        override public Cluster Cluster
        {
            get;
            protected set;
        }

        override public IEnumerable<LocalKeyspaceInfo> Keyspaces
		{
			get
			{
				throw new NotImplementedException();
			}
		}

		override public string Name
		{
			get;
			protected set;
		}

		private List<IEvent> _events = new List<IEvent>();
		override public IEnumerable<IEvent> Events { get { lock (this._events) { return this._events.ToArray(); } } }

		private List<IConfigurationLine> _configurations = new List<IConfigurationLine>();
		override public IEnumerable<IConfigurationLine> Configurations { get { lock (this._configurations) { return this._configurations.ToArray(); } } }

		private List<IDDL> _ddls = new List<IDDL>();
		override public IEnumerable<IDDL> DDLs { get { lock (this._ddls) { return this._ddls.ToArray(); } } }

		#endregion

		#region IEquatable
		override public bool Equals(string other)
		{
			return this.Name == other;
		}

		override public bool Equals(IDataCenter other)
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

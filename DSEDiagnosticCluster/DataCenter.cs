using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using Common.Patterns;
using System.Threading.Tasks;
using Common.Patterns.Tasks;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticLibrary
{
	public interface IDataCenter : IEquatable<IDataCenter>, IEquatable<string>
	{
		Cluster Cluster { get; }
		string Name { get; }

        IConfigurationLine ConfigurationMatch(string property, string value, ConfigTypes type, SourceTypes source);

        IEnumerable<INode> Nodes { get; }
		IEnumerable<IKeyspace> Keyspaces { get; }

        IKeyspace TryGetKeyspace(string ksName);

		INode TryGetNode(string nodeId);
        INode TryGetAddNode(INode node);

        IEnumerable<IEvent> Events { get; }
		IEnumerable<IConfigurationLine> Configurations { get; }
        IEnumerable<IConfigurationLine> GetConfigurations(INode node);

        IEnumerable<IDDL> DDLs { get; }

        object ToDump();
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

        public object ToDump()
        {
            return new { Name = this.Name,
                            Cluster = this.Cluster,
                            Nodes = this.Nodes,
                            Keyspaces = this.Keyspaces,
                            Configurations = this.Configurations,
                            Events = this.Events  };
        }

        #region IDataCenter
        virtual public Cluster Cluster
        {
            get { return this._nodes.First().Cluster; }
            protected set { throw new NotImplementedException(); }
        }

        virtual public IEnumerable<IKeyspace> Keyspaces
        {
            get
            {
                return Enumerable.Empty<IKeyspace>();
            }
        }

        private string _name = null;
        private string _className = null;
        virtual public string Name
        {
            get { return string.Format("{0}<{1}>", this._name, this._nodes.Count()); }
            protected set { throw new NotImplementedException(); }
        }

        protected CTS.List<INode> _nodes = new CTS.List<INode>();
        public IEnumerable<INode> Nodes
        {
            get { return this._nodes; }
        }

        virtual public IKeyspace TryGetKeyspace(string ksName)
        {
            throw new NotImplementedException();
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

        public override IKeyspace TryGetKeyspace(string ksName)
        {
            ksName = StringHelpers.RemoveQuotes(ksName?.Trim());

            return this.Keyspaces.FirstOrDefault(k => k.Name == ksName);
        }

        public INode TryGetAddNode(string nodeId)
		{
			if(!NodeIdentifier.ValidNodeIdName(nodeId))
			{ return null; }

            this._nodes.Lock();
            try
            {
                var node = this._nodes.UnSafe.FirstOrDefault(n => n.Equals(nodeId));

                if (node == null)
                {
                    node = new Node(this, nodeId);
                    this._nodes.UnSafe.Add(node);
                }

                return node;
            }
            finally
            {
                this._nodes.UnLock();
            }

		}

		public INode TryGetAddNode(NodeIdentifier nodeId)
		{
			if (nodeId == null)
			{ return null; }

            this._nodes.Lock();
            try
            {
                var node = this._nodes.UnSafe.FirstOrDefault(n => n.Equals(nodeId));

                if (node == null)
                {
                    node = new Node(this, nodeId);
                    this._nodes.UnSafe.Add(node);
                }

                return node;
            }
            finally
            {
                this._nodes.UnLock();
            }
		}

		override public INode TryGetAddNode(INode node)
		{
			if (node == null)
			{ return null; }

            this._nodes.Lock();
            try
            {
                if (this._nodes.UnSafe.Contains(node))
                {
                    node = this._nodes.UnSafe.First(n => n.Equals(node));
                }
                else
                {
                    this._nodes.UnSafe.Add(node);
                }

                if (node.DataCenter == null)
                {
                    ((Node)node).SetNodeToDataCenter(this);
                }

                return node;
            }
            finally
            {
                this._nodes.UnLock();
            }
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

        public new object ToDump()
        {
            return new { Name = this.Name,
                            Cluster = this.Cluster,
                            Nodes = this.Nodes,
                            Keyspaces = this.Keyspaces,
                            DDL = this.DDLs,
                            Configurations = this.Configurations,
                            Events = this.Events };
        }

        #region IDataCenter
        override public Cluster Cluster
        {
            get;
            protected set;
        }

        override public IEnumerable<IKeyspace> Keyspaces
		{
			get
			{
                return this.Cluster.GetKeyspaces(this);
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

		override public IEnumerable<IDDL> DDLs
        {
            get
            {
                return this.Keyspaces.Concat(this.Keyspaces.SelectMany(k => k.DDLs));
            }
        }

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

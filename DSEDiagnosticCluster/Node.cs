using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net;
using Common;
using Common.Patterns;
using Common.Patterns.TimeZoneInfo;

namespace DSEDiagnosticLibrary
{
	public sealed class NodeIdentifier : IEquatable<NodeIdentifier>, IEquatable<IPAddress>, IEquatable<string>
	{
		private NodeIdentifier()
		{
			this._addresses = new System.Collections.Concurrent.ConcurrentBag<IPAddress>();
		}
		public NodeIdentifier(IPAddress ipAddress)
		{
			this._addresses = new System.Collections.Concurrent.ConcurrentBag<IPAddress>() { ipAddress };
		}
		public NodeIdentifier(string hostName)
		{
			this.HostName = hostName;
		}

		#region Methods
		public IPAddress AddIPAddress(IPAddress ipAddress)
		{
			if(ipAddress != null
				&& !this._addresses.Contains(ipAddress))
			{
				this._addresses.Add(ipAddress);
			}
			return ipAddress;
		}

		/// <summary>
		/// Adds the IPAdresses from nodeIdentifier into this instance.
		/// </summary>
		/// <param name="nodeIdentifier"></param>
		/// <returns>
		/// Returns the first IPAddress of the addresses in nodeIdentifier that are not within the addresses in this instance.
		/// If null no addresses were found to be different between the instances or nodeIdentifier is null.
		/// </returns>
		public IPAddress AddIPAddress(NodeIdentifier nodeIdentifier)
		{
			if (nodeIdentifier != null)
			{
				var differences = nodeIdentifier._addresses.Complement(this._addresses);

				differences.ForEach(a => this._addresses.Add(a));

				return differences.FirstOrDefault();
			}

			return null;
		}

		public IPAddress AddIPAddress(string strIPAddress)
		{
			if (!string.IsNullOrEmpty(strIPAddress))
			{
				IPAddress ipAddress;
				if(IPAddress.TryParse(strIPAddress, out ipAddress))
				{
					return this.AddIPAddress(ipAddress);
				}
			}

			return null;
		}

		static Regex IPAddressRegEx = new Regex(LibrarySettings.IPAdressRegEx,
													RegexOptions.Compiled);

		public IPAddress SetIPAddressOrHostName(string strIPAddressOrHostName, bool tryParseAsIPAddress = true)
		{
			IPAddress ipAddress = null;

			if (!string.IsNullOrEmpty(strIPAddressOrHostName))
			{
                if (tryParseAsIPAddress && IPAddress.TryParse(strIPAddressOrHostName, out ipAddress))
                {
                    var ipAddressStr = ipAddress.ToString();

                    if (ipAddressStr == "127.0.0.1"
                            || ipAddressStr == "::1"
                            || ipAddressStr == "0:0:0:0:0:0:0:1")
                    {
                        ipAddress = null;
                    }
                    else
                    {
                        this.AddIPAddress(ipAddress);
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(this.HostName))
                    {
                        this.HostName = strIPAddressOrHostName;
                    }
                    else if(NodeIdentifier.HostNameEqual(this.HostName, strIPAddressOrHostName))
                    {
                        var hostParts = this.HostName.Count(c => c == '.');
                        var otherParts = strIPAddressOrHostName.Count(c => c == '.');

                        if(otherParts > hostParts)
                        {
                            this.HostName = strIPAddressOrHostName;
                        }
                    }
                }
			}

			return ipAddress;
		}

        public bool IsValid()
        {
            if(this.Addresses.Count() == 1)
            {
                return ValidNodeIdName(this.Addresses.First().ToString());
            }

            return this.HostName?.ToLower() != "localhost";
        }

        #endregion

        #region Static Methods

        [Flags]
        public enum CreateNodeIdentiferParsingOptions
        {
            IPAddressScan = 0x0001,
            HostNameScan = 0x0002,
            NodeNameEmbedded = 0x0008,
            IPAddressOrHostNameScan = IPAddressScan | HostNameScan,
            IPAddressOrHostNameEmbeddedScan = IPAddressScan | HostNameScan | NodeNameEmbedded
        }

        public static bool ValidNodeIdName(string possibleNodeName)
        {
            if (string.IsNullOrEmpty(possibleNodeName)
                     || possibleNodeName == "127.0.0.1"
                     || possibleNodeName == "0.0.0.0"
                     || possibleNodeName == "::1"
                     || possibleNodeName == "0:0:0:0:0:0:0:1"
                     || possibleNodeName.ToLower() == "localhost")
            {
                return false;
            }

            return true ;
        }

        /// <summary>
		/// Evaluates possibleNodeName to determine a Node&apos;s IPAdress (V4 or V6) or HostName.
		/// If a nodes&apos;s name is an IPAdress it can be embeded anywhere in the name.
		/// If it is a host name it must be at the beginning of the name separated by either a space (e.g., &quot;hostname filename.ext&quot;) or plus sign (e.g., hostname+filename.ext)
		/// If it is not an IPAdress or embedded host name, it is assumed possibleNodeName is a host name.
		/// </summary>
		/// <param name="possibleNodeName">string that is evaluted to determine if it is either an IPAdress or host name.</param>
		/// <param name="options">
		/// </param>
		/// <returns>
		/// Returns null if possibleNodeName is null, empty, or not valid (e.g., 127.0.0.1, hostname, etc.).
		/// Returns a NodeIdentifier instance with either an IPAdress or host name.
		/// </returns>
		public static NodeIdentifier CreateNodeIdentifer(string possibleNodeName, CreateNodeIdentiferParsingOptions options = CreateNodeIdentiferParsingOptions.IPAddressOrHostNameEmbeddedScan)
        {
            IPAddress ipAddress = null;

            if (ValidNodeIdName(possibleNodeName))
            {
                if (options.HasFlag(CreateNodeIdentiferParsingOptions.NodeNameEmbedded))
                {
                    if (options.HasFlag(CreateNodeIdentiferParsingOptions.IPAddressScan))
                    {
                        var ipMatch = IPAddressRegEx.Match(possibleNodeName);

                        if (ipMatch.Success)
                        {
                            if (IPAddress.TryParse(ipMatch.Value, out ipAddress))
                            {
                                return new NodeIdentifier(ipAddress);
                            }
                        }
                    }

                    if (options.HasFlag(CreateNodeIdentiferParsingOptions.HostNameScan))
                    {
                        var nameParts = possibleNodeName.Split(LibrarySettings.HostNamePathNameCharSeparators);

                        if (nameParts.Length > 1)
                        {
                            return new NodeIdentifier(nameParts[0]);
                        }
                    }

                    return null;
                }

                if (options.HasFlag(CreateNodeIdentiferParsingOptions.IPAddressScan))
                {
                    if (IPAddress.TryParse(possibleNodeName, out ipAddress))
                    { return new NodeIdentifier(ipAddress); }
                }

                if (options.HasFlag(CreateNodeIdentiferParsingOptions.HostNameScan))
                {
                    return new NodeIdentifier(possibleNodeName);
                }
            }

            return null;
        }

        /// <summary>
        /// Creates a new NodeIdentifier based on strIPAddressOrHostName
        /// </summary>
        /// <param name="strIPAddressOrHostName">
        /// This methods does not perform any type of scans, it just takes the argument and determines if it is an IP Address. If not it becomes the host name.
        /// </param>
        /// <param name="tryParseAsIPAddress"></param>
        /// <returns>
        /// Returns a NodeIdentifier instance or thrown an ArgumentException id the Id name is not valid.
        /// </returns>
        /// <exception cref="System.ArgumentException">
        /// Thrown is the Id string is not valid
        /// </exception>
        public static NodeIdentifier Create(string strIPAddressOrHostName, bool tryParseAsIPAddress = true)
        {
            if (NodeIdentifier.ValidNodeIdName(strIPAddressOrHostName))
            {
                var instance = new NodeIdentifier();

                instance.SetIPAddressOrHostName(strIPAddressOrHostName, tryParseAsIPAddress);

                return instance;
            }

            throw new ArgumentException(string.Format("NodeIdentifier Name \"{0}\" is not valid", strIPAddressOrHostName, "strIPAddressOrHostName"));
        }

        public static bool operator ==(NodeIdentifier a, NodeIdentifier b)
        {
            return ReferenceEquals(a, b) || (!ReferenceEquals(a, null) && a.Equals(b));
        }

        public static bool operator !=(NodeIdentifier a, NodeIdentifier b)
        {
            if (ReferenceEquals(a, null) && !ReferenceEquals(b, null)) return true;
            if (ReferenceEquals(b, null) && !ReferenceEquals(a, null)) return true;
            if (ReferenceEquals(a, b)) return false;

            return !a.Equals(b);
        }

        #endregion

        public string HostName { get; set; }
		private System.Collections.Concurrent.ConcurrentBag<IPAddress> _addresses { get; set; }
		public IEnumerable<IPAddress> Addresses { get { return this._addresses; } }

		#region IEquatable
		public bool Equals(NodeIdentifier other)
		{
            if (ReferenceEquals(other, null)) return false;

            if (ReferenceEquals(this, other)) return true;

			if(HostNameEqual(this.HostName, other.HostName)) return true;

			if(this._addresses != null
					&& other._addresses != null)
			{
				return this._addresses.Contains(other._addresses);
			}

			return false;
		}

		public bool Equals(IPAddress other)
		{
			if (this._addresses != null
					&& other != null)
			{
				return this._addresses.Contains(other);
			}

			return false;
		}

		public bool Equals(string other)
		{
            if (HostNameEqual(this.HostName, other)) return true;

			IPAddress address;

			if(IPAddress.TryParse(other, out address))
			{
				return this.Equals(address);
			}

            return false;
		}

        /// <summary>
        /// Returns false if either or all argument(s) is/are null or string.empty
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static bool HostNameEqual(string a, string b)
        {
            if (string.IsNullOrEmpty(a)) return false;
            if (string.IsNullOrEmpty(b)) return false;
            if (a == b) return true;

            var thisHostParts = a.Split('.');
            var otherHostParts = b.Split('.');

            //Make sure it is not something like
            // host.a.b.c and host.d.e.f
            // so we should only check something like host.a.b.com == host
            if (thisHostParts.Length == 1 || otherHostParts.Length == 1)
            {
                //Just check for only host name (first value in array)
                // host.a.b.com == host
                if (thisHostParts[0] == otherHostParts[0])
                {
                    return true;
                }
            }

            return false;
        }

		public override bool Equals(object obj)
		{
			if(obj is NodeIdentifier)
			{ return this.Equals((NodeIdentifier)obj); }

			if (obj is IPAddress)
			{ return this.Equals((IPAddress)obj); }

			if (obj is string)
			{ return this.Equals((string)obj); }

			return false;
		}

		#endregion

		public override string ToString()
		{
			return this.ToString("NodeIdentifier");
		}

		public string ToString(string className)
		{
            string nodeAddress = null;

			if (this._addresses != null)
			{
                if (this._addresses.Count == 1)
                {
                    nodeAddress = this._addresses.First().ToString();
                }
                else
                {
                    nodeAddress = string.Join(",", this._addresses);
                }
			}

            if (string.IsNullOrEmpty(this.HostName))
            {
                if(!string.IsNullOrEmpty(nodeAddress))
                {
                    return string.Format("{0}{{{1}}}", className, nodeAddress);
                }
            }
            else
            {
                return string.IsNullOrEmpty(nodeAddress)
                        ? string.Format("{0}{{{1}}}", className, this.HostName)
                        : string.Format("{0}{{{1}:{2}}}", className, this.HostName, nodeAddress);
            }

            return base.ToString();
		}

		public override int GetHashCode()
		{
			if(string.IsNullOrEmpty(this.HostName))
			{
				return this._addresses.GetHashCode();
			}

			return this.HostName.GetHashCode();
		}

    }

	public sealed class MachineInfo
	{
		public sealed class CPUInfo
		{
			public string Architecture;
			public uint? Cores;
		}

		public sealed class CPULoadInfo
		{
			public UnitOfMeasure Average;
			public UnitOfMeasure Idle;
			public UnitOfMeasure System;
			public UnitOfMeasure User;
		}

		public sealed class MemoryInfo
		{
			public UnitOfMeasure PhysicalMemory;
			public UnitOfMeasure Available;
			public UnitOfMeasure Cache;
			public UnitOfMeasure Buffers;
			public UnitOfMeasure Shared;
			public UnitOfMeasure Free;
			public UnitOfMeasure Used;
		}

		public sealed class JavaInfo
		{
			public struct MemoryInfo
			{
				public UnitOfMeasure Committed;
				public UnitOfMeasure Initial;
				public UnitOfMeasure Maximum;
				public UnitOfMeasure Used;
			}

			public string Vendor;
			public int? Model;
			public string RuntimeName;
			public string Version;
			public string GCType;
			public MemoryInfo NonHeapMemory;
			public MemoryInfo HeapMemory;
		}

		public sealed class NTPInfo
		{
            public IPAddress NTPServer;
            public int? Stratum;
            public UnitOfMeasure Correction;
            public UnitOfMeasure Polling;
            public UnitOfMeasure MaximumError;
            public UnitOfMeasure EstimatedError;
            public int? TimeConstant;
            public UnitOfMeasure Precision;
            public UnitOfMeasure Frequency;
            public UnitOfMeasure Tolerance;
		}

        public MachineInfo()
        {
            this.CPU = new CPUInfo();
            this.CPULoad = new CPULoadInfo();
            this.Java = new JavaInfo();
            this.Memory = new MemoryInfo();
            this.NTP = new NTPInfo();
        }

		public CPUInfo CPU;
		public string OS;
		public string OSVersion;
        public string InstanceType;
		public IZone TimeZone;
		public CPULoadInfo CPULoad;
		public MemoryInfo Memory;
		public JavaInfo Java;
		public NTPInfo NTP;
	}

	public sealed class DSEInfo
	{
		[Flags]
		public enum InstanceTypes
		{
			Unkown = 0,
			Cassandra = 0x0001,
			Search = 0x0002,
			Analytics = 0x0004,
			TT = 0x0008,
			JT = 0x0010,
            Graph = 0x0020,
			Analytics_TT = Analytics | TT,
			Analytics_JS = Analytics | JT
		}

		public enum DSEStatuses
		{
			Unknown = 0,
			Up,
			Down
		}

		public sealed class VersionInfo
		{
			public Version DSE;
			public Version Cassandra;
			public Version Search;
			public Version Analytics;
			public Version OpsCenterAgent;
		}

		public sealed class TokenRangeInfo
		{
            private TokenRangeInfo() { }
            public TokenRangeInfo(long startToken, long endToken, UnitOfMeasure loadToken)
            {
                this.StartRange = startToken;
                this.EndRange = endToken;
                this.Load = loadToken;

                if (this.StartRange > 0 && this.EndRange < 0)
                {
                    this.Slots = (ulong)((this.EndRange - long.MinValue)
                                            + (long.MaxValue - this.StartRange));
                    this.WrapsRange = true;
                }
                else
                {
                    this.Slots = (ulong) (this.EndRange - this.StartRange);
                }
            }
            public TokenRangeInfo(string startToken, string endToken, string loadToken)
            {
                if(!long.TryParse(startToken, out this.StartRange))
                {
                    throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a long Start Token value.", startToken), "startToken");
                }
                if (!long.TryParse(endToken, out this.EndRange))
                {
                    throw new ArgumentException(string.Format("Could not parse value \"{0}\" into a long End Token value.", endToken), "endToken");
                }
                this.Load = UnitOfMeasure.Create(loadToken);

                if (this.StartRange > 0 && this.EndRange < 0)
                {
                    this.Slots = (ulong)((this.EndRange - long.MinValue)
                                            + (long.MaxValue - this.StartRange));
                    WrapsRange = true;
                }
                else
                {
                    this.Slots = (ulong)(this.EndRange - this.StartRange);
                }
            }

            public bool WrapsRange = false;

            public long StartRange; //Start Token (exclusive)
			public long EndRange; //End Token (inclusive)
			public ulong Slots;
			public UnitOfMeasure Load;

            /// <summary>
            /// Checks to determine if checkRange is within this range.
            /// </summary>
            /// <param name="checkRange"></param>
            /// <returns></returns>
            public bool IsWithinRange(TokenRangeInfo checkRange)
            {
                if(checkRange.EndRange <= this.EndRange)
                {
                    if(checkRange.EndRange > this.StartRange)
                    {
                        return true;
                    }
                }

                if (checkRange.StartRange > this.StartRange)
                {
                    if (checkRange.StartRange < this.EndRange)
                    {
                        return true;
                    }
                }

                return false;
            }

            public override string ToString()
            {
                return string.Format("TokenRangeInfo{{Range:({0},{1}], Slots:{2:###,###,###,###,##0}, Load:{3}}}",
                                        this.StartRange,
                                        this.EndRange,
                                        this.Slots,
                                        this.Load);
            }
        }

        public DSEInfo()
        {
            this.Versions = new VersionInfo();
        }

		public InstanceTypes InstanceType;
		public VersionInfo Versions;
        public Guid HostId;
		public string Rack;
		public DSEStatuses Statuses;
		public UnitOfMeasure StorageUsed;
		public UnitOfMeasure StorageUtilization;
		public string HealthRating;
		public UnitOfMeasure Uptime;
		public UnitOfMeasure Heap;
		public UnitOfMeasure OffHeap;
		public bool? VNodesEnabled;
		public uint? NbrVNodes;
		public uint? NbrExceptions;
		public bool? GossipEnabled;
		public bool? ThriftEnabled;
		public bool? NativeTransportEnabled;
        public bool? RepairServiceHasRan;
        public DateTimeRange RepairServiceRanRange;
        public string KeyCacheInformation;
		public string RowCacheInformation;
		public string CounterCacheInformation;

        private List<TokenRangeInfo> _tokenRanges = new List<TokenRangeInfo>();
		public IEnumerable<TokenRangeInfo> TokenRanges { get { return this._tokenRanges; } }

        public TokenRangeInfo AddTokenPair(string start, string end, string tokenLoad)
        {
            var range = new TokenRangeInfo(start, end, tokenLoad);

            if(this._tokenRanges.Any(r => r.IsWithinRange(range)))
            {
                throw new ArgumentException(string.Format("Range {0} is within Current Token Ranges for this node.", range), "start,end,load");
            }

            this._tokenRanges.Add(range);

            return range;
        }
	}

	public interface INode : IEquatable<INode>, IEquatable<NodeIdentifier>, IEquatable<IPAddress>, IEquatable<string>
	{
		Cluster Cluster { get; }
		IDataCenter DataCenter { get; }
		NodeIdentifier Id { get; }
		MachineInfo Machine { get; }
		DSEInfo DSE { get; }

		IEnumerable<IEvent> Events { get; }
		INode AssocateItem(IEvent eventItem);
		IEnumerable<IConfiguration> Configurations { get; }
		INode AssocateItem(IConfiguration eventItem);
		IEnumerable<IDDL> DDLs { get; }
		INode AssocateItem(IDDL eventItem);
	}

	public sealed class Node : INode
	{
		private static QueueProcessor<Tuple<Node, IEvent>> EventProcessQueue = new QueueProcessor<Tuple<Node, IEvent>>();
		private static QueueProcessor<Tuple<Node, IConfiguration>> ConfigProcessQueue = new QueueProcessor<Tuple<Node, IConfiguration>>();
		private static QueueProcessor<Tuple<Node, IDDL>> DDLProcessQueue = new QueueProcessor<Tuple<Node, IDDL>>();

		static Node()
		{
			EventProcessQueue.OnProcessMessageEvent += EventProcessQueue_OnProcessMessageEvent;
			EventProcessQueue.Name = "Node-IEvent";
			ConfigProcessQueue.OnProcessMessageEvent += ConfigProcessQueue_OnProcessMessageEvent;
			ConfigProcessQueue.Name = "Node-IConfiguration";
			DDLProcessQueue.OnProcessMessageEvent += DDLProcessQueue_OnProcessMessageEvent;
			DDLProcessQueue.Name = "Node-IDDL";
			DDLProcessQueue.StartQueue(false);
			ConfigProcessQueue.StartQueue(false);
			EventProcessQueue.StartQueue(false);
		}

		private Node()
		{
            this.DSE = new DSEInfo();
            this.Machine = new MachineInfo();
        }

		public Node(Cluster cluster, IDataCenter dataCenter, string nodeId)
            : this()
		{
			this.Cluster = cluster;
			this.DataCenter = dataCenter;
			this.Id = NodeIdentifier.Create(nodeId);
		}

		public Node(Cluster cluster, IDataCenter dataCenter, NodeIdentifier nodeId)
            : this()
		{
			this.Cluster = cluster;
			this.DataCenter = dataCenter;
			this.Id = nodeId;
		}

		public IDataCenter SetNodeToDataCenter(IDataCenter dataCenter)
		{
			if (dataCenter == null)
			{
				throw new ArgumentNullException("dataCenter cannot be null");
			}

			if(this.DataCenter == null)
			{
				lock(this._events)
				lock(this._configurations)
				lock(this._ddls)
				{
					this._events.ForEach(item => ((DataCenter)dataCenter).AssocateItem(item));
					this._configurations.ForEach(item => ((DataCenter)dataCenter).AssocateItem(item));
					this._ddls.ForEach(item => ((DataCenter)dataCenter).AssocateItem(item));

					this.DataCenter = dataCenter;
				}

				return dataCenter;
			}

			if(this.DataCenter.Equals(dataCenter))
			{
				return this.DataCenter;
			}

			throw new ArgumentOutOfRangeException("dataCenter",
													string.Format("Node \"{0}\" has an existing DataCenter of \"{1}\". Trying to set it to DataCenter \"{2}\". This is not allowed once a datacenter is set.",
																	this.Id,
																	this.DataCenter.Name,
																	dataCenter?.Name));
		}

		#region INode
		public Cluster Cluster
		{
			get;
			private set;
		}

		public IDataCenter DataCenter
		{
			get;
			private set;
		}

		public DSEInfo DSE
		{
			get;
		}

		public NodeIdentifier Id
		{
			get;
			private set;
		}

		public MachineInfo Machine
		{
			get;
		}

		private List<IEvent> _events = new List<IEvent>();
		public IEnumerable<IEvent> Events { get { lock (this._events) { return this._events.ToArray(); } } }

		private List<IConfiguration> _configurations = new List<IConfiguration>();
		public IEnumerable<IConfiguration> Configurations { get { lock (this._configurations) { return this._configurations.ToArray(); } } }

		private List<IDDL> _ddls = new List<IDDL>();
		public IEnumerable<IDDL> DDLs { get { lock (this._ddls) { return this._ddls.ToArray(); } } }

		public INode AssocateItem(IEvent eventItem)
		{
			if (eventItem != null)
			{
				EventProcessQueue.Enqueue(new Tuple<Node, IEvent>(this, eventItem));
				((DataCenter)this.DataCenter)?.AssocateItem(eventItem);
			}

			return this;
		}
		public INode AssocateItem(IConfiguration configItem)
		{
			if (configItem != null)
			{
				ConfigProcessQueue.Enqueue(new Tuple<Node, IConfiguration>(this, configItem));
				((DataCenter)this.DataCenter)?.AssocateItem(configItem);
			}

			return this;
		}
		public INode AssocateItem(IDDL ddlItem)
		{
			if (ddlItem != null)
			{
				DDLProcessQueue.Enqueue(new Tuple<Node, IDDL>(this, ddlItem));
				((DataCenter)this.DataCenter)?.AssocateItem(ddlItem);
			}

			return this;
		}
		#endregion

		#region IEquatable
		public bool Equals(IPAddress other)
		{
			return this.Id.Equals(other);
		}

		public bool Equals(string other)
		{
			return this.Id.Equals(other);
		}
		public bool Equals(INode other)
		{
			return other == null ? false : this.Id.Equals(other.Id);
		}

		public bool Equals(NodeIdentifier other)
		{
			return this.Id.Equals(other);
		}
		#endregion

		#region Overrides

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(this, obj))
			{ return true; }
			if(obj is INode)
			{ return this.Equals((INode)obj); }
			if(obj is string)
			{ return this.Equals((string)obj); }
			if(obj is NodeIdentifier)
			{ return this.Equals((NodeIdentifier)obj); }
			if (obj is IPAddress)
			{ return this.Equals((IPAddress)obj); }

			return false;
		}

		public override int GetHashCode()
		{
			return this.Id == null ? 0 : this.Id.GetHashCode();
		}

		public override string ToString()
		{
			return this.Id == null ? base.ToString() : this.Id.ToString("Node");
		}

		#endregion

		#region Processing Queue

		private static void EventProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<Node, IEvent>> sender, QueueProcessor<Tuple<Node, IEvent>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._events)
			{
				processMessageArgs.ProcessedMessage.Item1._events.Add(processMessageArgs.ProcessedMessage.Item2);
			}
		}

		private static void ConfigProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<Node, IConfiguration>> sender, QueueProcessor<Tuple<Node, IConfiguration>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._configurations)
			{
				processMessageArgs.ProcessedMessage.Item1._configurations.Add(processMessageArgs.ProcessedMessage.Item2);
			}
		}

		private static void DDLProcessQueue_OnProcessMessageEvent(QueueProcessor<Tuple<Node, IDDL>> sender, QueueProcessor<Tuple<Node, IDDL>>.MessageEventArgs processMessageArgs)
		{
			lock (processMessageArgs.ProcessedMessage.Item1._ddls)
			{
				processMessageArgs.ProcessedMessage.Item1._ddls.Add(processMessageArgs.ProcessedMessage.Item2);
			}
		}

		#endregion
	}
}

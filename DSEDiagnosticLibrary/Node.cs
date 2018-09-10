using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Net;
using Newtonsoft.Json;
using Common;
using Common.Patterns;
using Common.Patterns.TimeZoneInfo;
using CTS = Common.Patterns.Collections.ThreadSafe;
using CMM = Common.Patterns.Collections.MemoryMapped;
using IMMLogValue = Common.Patterns.Collections.MemoryMapped.IMMValue<DSEDiagnosticLibrary.ILogEvent>;

namespace DSEDiagnosticLibrary
{
    [JsonObject(MemberSerialization.OptOut)]
	public sealed class NodeIdentifier : IEquatable<NodeIdentifier>, IEquatable<IPAddress>, IEquatable<string>
	{
		private NodeIdentifier()
		{
			this._addresses = new System.Collections.Concurrent.ConcurrentBag<IPAddress>();
            this._hostnames = new CTS.List<string>();
		}
		public NodeIdentifier(IPAddress ipAddress)
            : this()
		{
            if (ipAddress != null) this._addresses.Add(ipAddress);
        }
		public NodeIdentifier(string hostName) 
            : this()
		{
			this.HostName = hostName?.Trim();
            if (this.HostName == string.Empty) this.HostName = null;
            if (this.HostName != null) this._hostnames.Add(this.HostName);
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
            strIPAddress = strIPAddress?.Trim();
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

		public static readonly Regex IPAddressRegEx = new Regex(LibrarySettings.IPAdressRegEx,
													                RegexOptions.Compiled);

		public IPAddress SetIPAddressOrHostName(string strIPAddressOrHostName, bool tryParseAsIPAddress = true)
		{
			IPAddress ipAddress = null;

            strIPAddressOrHostName = strIPAddressOrHostName?.Trim();

            if (ValidNodeIdName(strIPAddressOrHostName))
			{
                if (tryParseAsIPAddress && IPAddress.TryParse(strIPAddressOrHostName, out ipAddress))
                {
                    this.AddIPAddress(ipAddress);                    
                }
                else
                {
                    bool isLocked = false;
                    this._hostnames.Lock(ref isLocked);
                    try
                    {
                        var hostPos = this._hostnames.UnSafe.FindIndex(n => HostNameEqual(n, strIPAddressOrHostName));

                        if (hostPos >= 0)
                        {
                            var hostParts = this._hostnames.UnSafe[hostPos].Count(c => c == '.');
                            var otherParts = strIPAddressOrHostName.Count(c => c == '.');

                            if (otherParts > hostParts)
                            {
                                if(this.HostName == this._hostnames.UnSafe[hostPos])
                                {
                                    this.HostName = strIPAddressOrHostName;
                                }
                                this._hostnames.UnSafe[hostPos] = strIPAddressOrHostName;                                
                            }
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(this.HostName))
                            {
                                this.HostName = strIPAddressOrHostName;
                            }
                            else
                            {
                                var hostParts = this.HostName.Count(c => c == '.');
                                var otherParts = strIPAddressOrHostName.Count(c => c == '.');

                                if (otherParts > hostParts)
                                {
                                    this.HostName = strIPAddressOrHostName;
                                }
                            }
                            this._hostnames.UnSafe.Add(strIPAddressOrHostName);
                        }
                    }
                    finally
                    {
                        if (isLocked) this._hostnames.UnLock();
                    }
                    
                }
			}

			return ipAddress;
		}

        /// <summary>
        /// Returns Nodes IP4 address first, Host Name second, and IP6 address only if other two not defined.
        /// </summary>
        /// <returns></returns>
        public string NodeName()
        {
            IPAddress nodeAddress = null;

            if (this._addresses != null)
            {
                if (this._addresses.Count == 1)
                {
                    nodeAddress = this._addresses.First();
                }
                else
                {
                    nodeAddress = this._addresses.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);

                    if(nodeAddress == null)
                    {
                        nodeAddress = this._addresses.First();
                    }
                }
            }

            if(nodeAddress == null)
            {
                return this.HostName;
            }
            else if (nodeAddress.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
            {
                return nodeAddress.ToString();
            }
            else if(!string.IsNullOrEmpty(this.HostName))
            {
                return this.HostName;
            }

            return nodeAddress.ToString();
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
            possibleNodeName = possibleNodeName?.Trim();

            if (string.IsNullOrEmpty(possibleNodeName)
                     || possibleNodeName == "127.0.0.1"
                     || possibleNodeName == "0.0.0.0"
                     || possibleNodeName == "::1"
                     || possibleNodeName == "0:0:0:0:0:0:0:1"
                     || possibleNodeName.ToLower().StartsWith("localhost")
                     || possibleNodeName.ToLower().EndsWith("localhost")
                     || possibleNodeName.ToLower().StartsWith("loopback")
                     || possibleNodeName.ToLower().EndsWith("loopback"))
            {
                return false;
            }

            return true ;
        }

        public static bool DetermineIfNameHasIPAddressOrHostName(string possibleNodeName, bool checkForHostName = true)
        {
            var ipMatch = IPAddressRegEx.Match(possibleNodeName);

            if (!ipMatch.Success)
            {
                if (checkForHostName)
                {
                    var nameParts = possibleNodeName.Split(LibrarySettings.HostNamePathNameCharSeparators);

                    return nameParts.Length > 1;
                }

                return false;
            }

            return true;
        }

        /// <summary>
		/// Evaluates possibleNodeName to determine a Node&apos;s IPAdress (V4 or V6) or HostName.
		/// If a nodes&apos;s name is an IPAdress it can be embeded anywhere in the name.
		/// If it is a host name it must be at the beginning of the name separated by either a &quot;@&quot; (e.g., &quot;hostname@filename.ext&quot;) or plus sign (e.g., hostname+filename.ext)
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

            possibleNodeName = possibleNodeName?.Trim();

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

        [JsonProperty(PropertyName = "HostNames")]
        private IEnumerable<string> datamemberHostNames
        {
            get { return this._hostnames; }
            set
            {
                this._hostnames = value == null
                     ? null
                     : new Common.Patterns.Collections.ThreadSafe.List<string>(value);
            }
        }
        private Common.Patterns.Collections.ThreadSafe.List<string> _hostnames { get; set; }
       
        public string HostName { get; private set; }
        [JsonIgnore]
        public IEnumerable<string> HostNames { get { return this._hostnames; } }

        [JsonProperty(PropertyName= "Addresses")]
        private IEnumerable<string> datamemberAddresses
        {
            get { return this._addresses?.Select(a => a.ToString()); }
            set
            {
                this._addresses = value == null
                     ? null
                     : new System.Collections.Concurrent.ConcurrentBag<IPAddress>(value.Select(a => IPAddress.Parse(a)));
            }
        }
        private System.Collections.Concurrent.ConcurrentBag<IPAddress> _addresses { get; set; }
        [JsonIgnore]
        public IEnumerable<IPAddress> Addresses { get { return this._addresses; } }

        public bool HostNameExists(string checkHostName)
        {
            if (this._hostnames.Any(n => HostNameEqual(n, checkHostName))) return true;
            
            return false;
        }

        #region IEquatable
        public bool Equals(NodeIdentifier other)
		{
            if (ReferenceEquals(other, null)) return false;

            if (ReferenceEquals(this, other)) return true;

            //Prefer IP Address
            if (this._addresses.HasAtLeastOneElement() && other._addresses.HasAtLeastOneElement())
            {
                if (this._addresses.Contains(other._addresses)) return true;
                return false;
            }

            if (this._hostnames.Any(other._hostnames, (x, y) => HostNameEqual(x, y))) return true;

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
            if (this._hostnames.Any(n => HostNameEqual(n, other))) return true;

            if (this._addresses.HasAtLeastOneElement())
            {
                IPAddress address;

                if (IPAddress.TryParse(other, out address))
                {
                    return this.Equals(address);
                }
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
            a = a?.Trim();
            b = b?.Trim();

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

        public NodeIdentifier Clone(bool onlyIPAddress = false, bool onlyFirstItem = false)
        {
            var newId = new NodeIdentifier();

            if (onlyIPAddress && this._addresses.HasAtLeastOneElement())
            {
                if (onlyFirstItem)
                {
                    newId._addresses.Add(this._addresses.First());
                }
                else
                {
                    this._addresses.ForEach(a => newId._addresses.Add(a));
                }
            }
            else
            {
                if (onlyFirstItem)
                {
                    if (this._hostnames.HasAtLeastOneElement())
                    {
                        newId._hostnames.Add(this._hostnames.First());
                        newId.HostName = newId._hostnames.First();
                    }
                    if(this._addresses.HasAtLeastOneElement())
                        newId._addresses.Add(this._addresses.First());                    
                }
                else
                {
                    newId.HostName = this.HostName;
                    this._hostnames.ForEach(n => newId._hostnames.Add(n));
                    this._addresses.ForEach(a => newId._addresses.Add(a));
                    newId._hashCode = this._hashCode;
                }
            }
           
            return newId;
        }

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
                    nodeAddress = string.Join(", ", this._addresses);
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
                        : string.Format("{0}{{{1}, {2}}}", className, this.HostName, nodeAddress);
            }

            return base.ToString();
		}

        [JsonProperty(PropertyName="HashCode")]
        private int _hashCode = 0;
		public override int GetHashCode()
		{
            if (this._hashCode != 0) return this._hashCode;

			if(string.IsNullOrEmpty(this.HostName))
			{
                if (this._addresses.Count == 0) return 0;

                return this._hashCode = this._addresses.Select(a => a.GetHashCode()).Min();
			}

			return this.HostName.GetHashCode();
		}

        public object ToDump()
        {
            return new { Host = this.HostName, IPAddresses = string.Join(",", this.Addresses.Select(i => i.ToString())) };
        }

    }

    [JsonObject(MemberSerialization.OptOut)]
	public sealed class MachineInfo
	{
        [JsonObject(MemberSerialization.OptOut)]
        public sealed class CPUInfo
		{
			public string Architecture;
            public uint? Cores;
		}

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class CPULoadInfo
		{
            public UnitOfMeasure Average;
            public UnitOfMeasure Idle;
            public UnitOfMeasure System;
            public UnitOfMeasure User;
		}

        [JsonObject(MemberSerialization.OptOut)]
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

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class JavaInfo
		{
            [JsonObject(MemberSerialization.OptOut)]
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

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class NTPInfo
		{
            [JsonConverter(typeof(IPAddressJsonConverter))]
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

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class DeviceInfo
        {
            public IEnumerable<KeyValuePair<string, UnitOfMeasure>> Free;
            public IEnumerable<KeyValuePair<string, UnitOfMeasure>> Used;
            public IEnumerable<KeyValuePair<string, decimal>> PercentUtilized;
        }

        public MachineInfo(INode assocatedNode)
        {
            this._assocatedNode = assocatedNode;
            this.CPU = new CPUInfo();
            this.CPULoad = new CPULoadInfo();
            this.Java = new JavaInfo();
            this.Memory = new MemoryInfo();
            this.NTP = new NTPInfo();
            this.Devices = new DeviceInfo();
        }
        private INode _assocatedNode;

        public CPUInfo CPU;
        public string OS;
        public string OSVersion;
        public string Kernel;
        public string CloudVMType;
        public string Placement;

        /// <summary>
        /// Returns a time zone instance depending on if ExplictTimeZone is set. If not set the default time zone is used.
        /// The default time zone can be defined at the node, DC, or cluster levels.
        /// </summary>
        /// <seealso cref="ExplictTimeZone"/>
        /// <seealso cref="TimeZoneName"/>
        /// <seealso cref="DefaultAssocItemToTimeZone"/>
        /// <seealso cref="DataCenter.DefaultTimeZones"/>
        /// <seealso cref="Cluster.DefaultTimeZone"/>
        [JsonIgnore]
        public IZone TimeZone
        {
            get
            {
                if (this._explictTimeZone == null)
                {
                    if (string.IsNullOrEmpty(this._timezoneName))
                    {
                        if (Node.DefaultTimeZones != null && Node.DefaultTimeZones.Length > 0)
                        {                            
                            this.ExplictTimeZone = Node.DefaultTimeZones.FindDefaultTimeZone(this._assocatedNode.Id.NodeName());
                        }

                        if (this._explictTimeZone == null)
                        {
                            this.UsesDefaultTZ = true;
                            return this._assocatedNode.DataCenter?.DefaultMajorityTimeZone;
                        }
                    }
                    else
                    {
                        this.UsesDefaultTZ = false;
                        this._explictTimeZone = StringHelpers.FindTimeZone(this._timezoneName);
                    }
                }
                return this._explictTimeZone;
            }            
        }

        /// <summary>
        /// If true the default time zone from the DC is used.
        /// </summary>
        [JsonIgnore]
        public bool UsesDefaultTZ { get; private set; } = true;

        private Common.Patterns.TimeZoneInfo.IZone _explictTimeZone = null;
        /// <summary>
        /// This is the time zone that has been explicitly defined. This will also set TimeZoneName.
        /// </summary>
        /// <seealso cref="TimeZoneName"/>
        public Common.Patterns.TimeZoneInfo.IZone ExplictTimeZone
        {
            get { return this._explictTimeZone; }
            set
            {
                if (value == null)
                    this.UsesDefaultTZ = true;
                else
                    this.UsesDefaultTZ = false;

                this._explictTimeZone = value;
                this._timezoneName = this._explictTimeZone?.Name;
            }
        }

        private string _timezoneName;
        /// <summary>
        /// Sets the time zone. This will also set ExplictTimeZone property if the name is a valid IANA name.
        /// </summary>
        /// <seealso cref="ExplictTimeZone"/>
        public string TimeZoneName
        {
            get { return this._timezoneName; }
            set
            {
                if (value != this._timezoneName)
                {
                    if (string.IsNullOrEmpty(value))
                    {
                        this._timezoneName = null;
                        this._explictTimeZone = null;
                        this.UsesDefaultTZ = true;
                    }
                    else
                    {
                        this._timezoneName = value;
                        this._explictTimeZone = StringHelpers.FindTimeZone(this._timezoneName);
                        this.UsesDefaultTZ = false;
                    }
                }
            }
        }
        public CPULoadInfo CPULoad;
        public MemoryInfo Memory;
        public JavaInfo Java;
        public DeviceInfo Devices;
        public NTPInfo NTP;
	}

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class DSEInfo
	{
        public static DateTimeOffset? NodeToolCaptureTimestamp = null;

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
            AdvancedReplication = 0x0040,
            Hadoop = 0x0080,
            CFS = 0x0100,
            SW = 0x0200,
            SM = 0x0400,
            MultiInstance = 0x0800,
            Analytics_TT = Analytics | TT,
            Analytics_JT = Analytics | JT,
            Analytics_SM = Analytics | SM,
            Analytics_SW = Analytics | SW,
            Cassandra_JT = Cassandra | JT,
            SearchAnalytics = Search | Analytics,
            SearchAnalytics_TT = Search | Analytics | TT,
            SearchAnalytics_JT = Search | Analytics | JT,
            SearchAnalytics_SW = Search | Analytics | SW,
            SearchAnalytics_SM = Search | Analytics | SM
        }

		public enum DSEStatuses
		{
            Unknown = 0,
            Up,
            Down
		}

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class VersionInfo
		{
			public Version DSE;
            public Version Cassandra;
            public Version Search;
            public Version Analytics;
            public Version OpsCenterAgent;

            public static Version Parse(string version)
            {
                version = version?.Trim();
                if (string.IsNullOrEmpty(version)) return null;

                var parts = version.Split('.');

                if(parts.Length > 4)
                {
                    parts = new string[] { parts[0], parts[1], parts[2], parts[3] };
                }
                return new Version(string.Join(".", parts.Select(i => i.Trim())));
            }
		}
        
        [JsonObject(MemberSerialization.OptOut)]
        public sealed class DirectoryLocations
        {
            public string CassandraYamlFile;
            public string DSEYamlFile;
            public string HintsDir; //hints_directory
            public IEnumerable<string> DataDirs; //data_file_directories
            public string CommitLogDir; //commitlog_directory
            public string SavedCacheDir; //saved_caches_directory
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class DeviceLocations
        {            
            public IEnumerable<string> Others; 
            public IEnumerable<string> Data; 
            public string CommitLog; 
            public string SavedCache; 
        }

        public DSEInfo()
        {
            this.Versions = new VersionInfo();
            this.Locations = new DirectoryLocations();
            this.Devices = new DeviceLocations();
        }

        public InstanceTypes InstanceType;
        public VersionInfo Versions;
        public Guid HostId;
        /// <summary>
        /// Can be set in the DSE.yaml file (server_id) for multi-instance nodes to allow a group of DSE nodes on one physical server to be identified on that one physical server.
        /// </summary>
        public string PhysicalServerId;
        public string Rack;
        public string DataCenterSuffix;
        public DSEStatuses Statuses;
        public UnitOfMeasure StorageUsed;
        public UnitOfMeasure StorageUtilization;
        public string HealthRating;
        [JsonProperty(PropertyName = "CaptureTimestamp")]
        private DateTimeOffset? _captureTimestamp = null;
        [JsonIgnore]
        public DateTimeOffset? CaptureTimestamp
        {
            get { return this._captureTimestamp.HasValue ? this._captureTimestamp.Value : NodeToolCaptureTimestamp; }
            set
            {
                this._captureTimestamp = value;
            }
        }
        public DateTimeOffsetRange NodeToolDateRange;
        public UnitOfMeasure Uptime;
        public UnitOfMeasure Heap;
        public UnitOfMeasure HeapUsed;
        public UnitOfMeasure OffHeap;
        public bool? IsSeedNode; //seed_provider.parameters.seeds: "10.14.148.34,10.14.148.51"
        public bool? VNodesEnabled;
        public uint? NbrTokens;
        public uint? NbrExceptions;
        public bool? GossipEnabled;
        public bool? ThriftEnabled;
        public bool? NativeTransportEnabled;
        public bool? RepairServiceHasRan;
        public DateTimeRange RepairServiceRanRange;
        public UnitOfMeasure RepairedPercent;
        public DirectoryLocations Locations;
        public DeviceLocations Devices;
        public string EndpointSnitch;
        public string Partitioner;
        public string KeyCacheInformation;
        public string RowCacheInformation;
        public string CounterCacheInformation;
        public string ChunkCacheInformation;

        [JsonProperty(PropertyName="TokenRanges")]
        private List<TokenRangeInfo> _tokenRanges = new List<TokenRangeInfo>();
        [JsonIgnore]
        public IEnumerable<TokenRangeInfo> TokenRanges { get { return this._tokenRanges; } }
        
        public TokenRangeInfo AddTokenPair(string start, string end, string tokenLoad)
        {
            var range = TokenRangeInfo.CreateTokenRange(start, end, tokenLoad);

            if(this._tokenRanges.Any(r => r.IsWithinRange(range)))
            {
                throw new ArgumentException(string.Format("Range {0} is within Current Token Ranges for this node.", range), "start,end,load");
            }

            this._tokenRanges.Add(range);

            return range;
        }        
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class AnalyticsInfo
    {       
        public AnalyticsInfo()
        {
            this.RunningTotalProperties = new Dictionary<string, object>();
        }

        public IDictionary<string,object> RunningTotalProperties
        {
            get;
        }

    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class LogFileInfo
    {
        public static bool IsDebugLogFile(IFilePath logFile)
        {
            return logFile.Name.IndexOf(Properties.Settings.Default.DebugLogFileName, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        public LogFileInfo(IFilePath logFile,
                            DateTimeOffsetRange logfileDateRange,
                            int logItems,
                            IEnumerable<ILogEvent> orphanedEvents = null,
                            DateTimeOffsetRange logDateRange = null,
                            IEnumerable<DateTimeOffsetRange> nodeRestarts = null,
                            bool? isDebugLog = null)
        {
            this.LogFile = logFile;
            this.LogFileDateRange = logfileDateRange;
            this.LogItems = logItems;
            this.LogDateRange = logDateRange ?? logfileDateRange;
            this.LogFileSize = new UnitOfMeasure(logFile.FileInfo().Length, UnitOfMeasure.Types.Byte | UnitOfMeasure.Types.Storage);
            this.IsDebugLog = isDebugLog.HasValue ? isDebugLog.Value : IsDebugLogFile(this.LogFile);

            if (orphanedEvents != null)
            {
                this._orphanedEvents.AddRange(orphanedEvents);
            }

            if(nodeRestarts != null)
            {
                this._restarts.AddRange(nodeRestarts);
            }
        }

        public IFilePath LogFile { get; }
        /// <summary>
        /// The date range of the actual file
        /// </summary>
        public DateTimeOffsetRange LogFileDateRange { get; }
        /// <summary>
        /// The date range of the log entries
        /// </summary>
        public DateTimeOffsetRange LogDateRange { get; }
        public int LogItems { get; }
        public UnitOfMeasure LogFileSize { get; }

        [JsonProperty(PropertyName = "OrphanedEvents")]
        private IEnumerable<ILogEvent> datamemberOrphanedEvents
        {
            get { return this._orphanedEvents.UnSafe; }
            set { this._orphanedEvents = new CTS.List<ILogEvent>(value); }
        }

        private CTS.List<ILogEvent> _orphanedEvents = new CTS.List<ILogEvent>();
        [JsonIgnore]
        public IList<ILogEvent> OrphanedEvents { get { return this._orphanedEvents; } }

        private CTS.List<DateTimeOffsetRange> _restarts = new CTS.List<DateTimeOffsetRange>();
        [JsonIgnore]
        public IList<DateTimeOffsetRange> Restarts { get { return this._restarts; } }

        public bool IsDebugLog { get; }

        public override string ToString()
        {
            return string.Format("LogFileInfo{{{0}, LogFileRange={1}, LogRange={2}, Items={3}, Orphans={4}}}",
                                    this.LogFile.Name,
                                    this.LogFileDateRange,
                                    this.LogDateRange,
                                    this.LogItems,
                                    this.OrphanedEvents.Count);
        }
    }
	public interface INode : IEquatable<INode>, IEquatable<NodeIdentifier>, IEquatable<IPAddress>, IEquatable<string>
	{
		Cluster Cluster { get; }
		IDataCenter DataCenter { get; }
		NodeIdentifier Id { get; }
		MachineInfo Machine { get; }
		DSEInfo DSE { get; }

        AnalyticsInfo Analytics { get; }

		IEnumerable<IMMLogValue> LogEvents { get; }
        IEnumerable<IMMLogValue> LogEventsCache(LogCassandraEvent.ElementCreationTypes creationType, bool presistCache = false, bool safeRead = true);
        IEnumerable<IMMLogValue> LogEventsRead(LogCassandraEvent.ElementCreationTypes creationType, bool safeRead = true);

        INode AssociateItem(IMMLogValue eventItems);
        IMMLogValue AssociateItem(ILogEvent eventItems);
        IEnumerable<IAggregatedStats> AggregatedStats { get; }
        INode AssociateItem(IAggregatedStats aggregatedStat);

        IEnumerable<LogFileInfo> LogFiles { get; }
        INode AssociateItem(LogFileInfo logFileInfo);

        IEnumerable<IConfigurationLine> Configurations { get; }

        object ToDump();

        bool UpdateDSENodeToolDateRange();
	}

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class Node : INode
	{
        /// <summary>
        /// An array of nodes that are associated to a IANA time zone name.
        /// </summary>
        public static DefaultAssocItemToTimeZone[] DefaultTimeZones = null;
        
        static Node()
		{
		}

		private Node()
		{
            this.DSE = new DSEInfo();
            this.Machine = new MachineInfo(this);
            this.Analytics = new AnalyticsInfo();

            if (LibrarySettings.LogEventsAreMemoryMapped)
            {
                this._eventsCMM = new CMM.List<LogCassandraEvent, ILogEvent>();
            }
            else
            {
                this._eventsCTS = new CTS.List<IMMLogValue>();
            }        
        }

		public Node(IDataCenter dataCenter, string nodeId)
            : this()
		{
			this.DataCenter = dataCenter;
			this.Id = NodeIdentifier.Create(nodeId);            
        }

		public Node(IDataCenter dataCenter, NodeIdentifier nodeId)
            : this()
		{
			this.DataCenter = dataCenter;
			this.Id = nodeId;
		}

        internal Node(Cluster cluster, NodeIdentifier nodeId)
            : this()
        {
            this._defaultCluster = cluster;
            this.Id = nodeId;
        }

        internal Node(Cluster cluster, string nodeId)
            : this()
        {
            this._defaultCluster = cluster;
            this.Id = NodeIdentifier.Create(nodeId);
        }

        public IDataCenter SetNodeToDataCenter(IDataCenter dataCenter)
		{
			if (dataCenter == null)
			{
				throw new ArgumentNullException("dataCenter cannot be null");
			}

			if(this.DataCenter == null)
			{				
                lock(this)
                {
                    if(this.DataCenter == null)
                    {
                        this.DataCenter = dataCenter;                        
                        this._hashcode = 0;
                    }
                }
			}

			if(ReferenceEquals(this.DataCenter, dataCenter))
			{
                Cluster.RemoveNodeFromUnAssociatedList(this.Id);
                this._defaultCluster = null;
                return this.DataCenter;
			}

			throw new ArgumentOutOfRangeException("dataCenter",
													string.Format("Node \"{0}\" has an existing DataCenter of \"{1}\". Trying to set it to DataCenter \"{2}\". This is not allowed once a datacenter is set.",
																	this.Id,
																	this.DataCenter.Name,
																	dataCenter.Name));
		}

        internal Cluster SetCluster(Cluster associateCluster)
        {
            if (associateCluster != null && this._defaultCluster != null)
            {
                lock (this)
                {
                    if (this._defaultCluster != null && !ReferenceEquals(this._defaultCluster, associateCluster))
                    {
                        this._defaultCluster = associateCluster;
                        this._hashcode = 0;
                    }
                }
            }

            return this._defaultCluster;
        }

        #region INode
        [JsonProperty(PropertyName="DefaultCluster")]
        private Cluster _defaultCluster = null;
        [JsonIgnore]
        public Cluster Cluster
		{
			get { return this.DataCenter?.Cluster ?? this._defaultCluster; }
		}

        [JsonProperty(PropertyName="DefaultDC")]
        private IDataCenter _defaultDC = null;
        [JsonIgnore]
        public IDataCenter DataCenter
		{
			get
            {
                return this._defaultDC;
            }
			private set
            {
                if (!ReferenceEquals(this._defaultDC, value))
                {
                    lock (this)
                    {
                        this._defaultDC = value;
                        if (this._defaultDC != null)
                        {
                            this._defaultCluster = null;
                        }
                    }
                }
            }
		}

        public DSEInfo DSE
		{
			get;
		}

        public AnalyticsInfo Analytics
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

        /*[JsonProperty(PropertyName="LogEvents")]
        private IEnumerable<IMMLogValue> datamemberEvents
        {
            get { return this._events.UnSafe; }
            set { this._events = new CTS.List<LogCassandraEvent>(value); }
        }*/

        private CMM.List<LogCassandraEvent,ILogEvent> _eventsCMM;
        private CTS.List<IMMLogValue> _eventsCTS;

        [JsonIgnore]
        public IEnumerable<IMMLogValue> LogEvents { get { return this._eventsCMM?.ToEnumerable() ?? (IEnumerable<IMMLogValue>) this._eventsCTS; } }

        public IEnumerable<IMMLogValue> LogEventsCache(LogCassandraEvent.ElementCreationTypes creationType, bool presistCache = false, bool safeRead = true)
        {
            if(this._eventsCMM == null) return safeRead ? (IEnumerable<IMMLogValue>)this._eventsCTS : (IEnumerable<IMMLogValue>)this._eventsCTS.UnSafe;

            return this._eventsCMM.ToEnumerableCached((Common.Patterns.Collections.MemoryMapperElementCreationTypes)creationType, presistCache);
        }

        public IEnumerable<IMMLogValue> LogEventsRead(LogCassandraEvent.ElementCreationTypes creationType, bool safeRead = true)
        {
            if (this._eventsCMM == null) return safeRead ? (IEnumerable<IMMLogValue>)this._eventsCTS : (IEnumerable<IMMLogValue>)this._eventsCTS.UnSafe;

            return this._eventsCMM.ToEnumerable((Common.Patterns.Collections.MemoryMapperElementCreationTypes)creationType);
        }

        public INode AssociateItem(IMMLogValue eventItems)
        {
            if (this._eventsCMM == null)
                this._eventsCTS.Add(eventItems);
            else
                this._eventsCMM.Add(eventItems);

            return this;
        }

        public IMMLogValue AssociateItem(ILogEvent eventItems)
        {
            if (this._eventsCMM == null)
            {
                var mmValue = new CMM.MMValue<ILogEvent>(eventItems);
                this._eventsCTS.Add(mmValue);
                return mmValue;
            }

            return this._eventsCMM.AddElement((LogCassandraEvent) eventItems);
        }

        [JsonProperty(PropertyName = "AggregatedStats")]
        private IEnumerable<IAggregatedStats> datamemberAggregatedStats
        {
            get { return this._aggregatedStats.UnSafe; }
            set { this._aggregatedStats = new CTS.List<IAggregatedStats>(value); }
        }

        private CTS.List<IAggregatedStats> _aggregatedStats = new CTS.List<IAggregatedStats>();
        [JsonIgnore]
        public IEnumerable<IAggregatedStats> AggregatedStats { get { return this._aggregatedStats; } }
        public IEnumerable<IAggregatedStats> AggregatedStatsUnSafe { get { return this._aggregatedStats.UnSafe; } }

        public INode AssociateItem(IAggregatedStats aggregatedStat)
        {
            this._aggregatedStats.Add(aggregatedStat);
            return this;
        }

        [JsonProperty(PropertyName = "LogFiles")]
        private IEnumerable<LogFileInfo> datamemberLogFiles
        {
            get { return this._logFiles.UnSafe; }
            set { this._logFiles = new CTS.List<LogFileInfo>(value); }
        }

        private CTS.List<LogFileInfo> _logFiles = new CTS.List<LogFileInfo>();
        [JsonIgnore]
        public IEnumerable<LogFileInfo> LogFiles { get { return this._logFiles; } }
        public INode AssociateItem(LogFileInfo logFileInfo)
        {
            var fndItem = this._logFiles.FirstOrDefault(i => i.LogFile == logFileInfo.LogFile);

            if(fndItem != null)
            {
                if (fndItem.LogItems == 0 && (logFileInfo.LogItems != 0 || logFileInfo.LogDateRange != null))
                {
                    this._logFiles.Remove(fndItem);
                }
                else
                {
                    return this;
                }
            }

            this._logFiles.Add(logFileInfo);
            return this;
        }

        [JsonIgnore]
		public IEnumerable<IConfigurationLine> Configurations
        {
            get
            {
                return this.DataCenter.GetConfigurations(this);
            }
        }

        public bool UpdateDSENodeToolDateRange()
        {
            bool bResult = false;
            var machineTZ = this.Machine.TimeZone;

            if(!this.DSE.Uptime.NaN
                && this.DSE.CaptureTimestamp.HasValue)
            {
                DateTimeOffset refDTO;

                if (machineTZ == null)
                {
                    refDTO = this.DSE.CaptureTimestamp.Value;
                }
                else
                {
                    try
                    {
                        refDTO = Common.TimeZones.Convert(this.DSE.CaptureTimestamp.Value, machineTZ);
                    }
                    catch(System.Exception)
                    {
                        System.Threading.Thread.Sleep(1);
                        refDTO = Common.TimeZones.Convert(this.DSE.CaptureTimestamp.Value, machineTZ);
                    }
                }

                if (this.DSE.NodeToolDateRange == null || this.DSE.NodeToolDateRange.Max.Offset != refDTO.Offset)
                {
                    this.DSE.NodeToolDateRange = new DateTimeOffsetRange(refDTO - (TimeSpan)this.DSE.Uptime, refDTO);
                    bResult = true;
                }
            }

            return bResult;
        }

        public object ToDump()
        {
            return new { Id = this.Id,
                            DataCenter = this.DataCenter,
                            DSEInfo = this.DSE,
                            MachineInfo = this.Machine,
                            LogEvents = this.LogEvents,
                            Configurations = this.Configurations };
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

        [JsonProperty(PropertyName="HashCode")]
        private int _hashcode = 0;
		public override int GetHashCode()
		{
            unchecked
            {
                if (this._hashcode != 0) return this._hashcode;
                if (this.Id == null) return 0;
                if (this.Cluster.IsMaster) return this.Id.GetHashCode();

                return this._hashcode = this.DataCenter.GetHashCode() * 31 + this.Id.GetHashCode();
            }
		}

		public override string ToString()
		{
			return this.Id == null ? base.ToString() : this.Id.ToString("Node");
		}

        public static bool operator ==(Node a, Node b)
        {
            return ReferenceEquals(a, b) || (!ReferenceEquals(a, null) && a.Equals(b));
        }

        public static bool operator !=(Node a, Node b)
        {
            if (ReferenceEquals(a, null) && !ReferenceEquals(b, null)) return true;
            if (ReferenceEquals(b, null) && !ReferenceEquals(a, null)) return true;
            if (ReferenceEquals(a, b)) return false;

            return !a.Equals(b);
        }

        #endregion

	}
}

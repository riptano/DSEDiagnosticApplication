using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using Common;
using System.Threading;

namespace DSEDiagnosticFileParser
{
    public sealed class CLogLineTypeParser
    {
        #region private properties
        private class CacheInfo
        {
            public System.Reflection.MethodInfo KeyMethodInfo;
            public bool KeyMethodInfoIsMethod;
            public bool KeyIsGroupName;
            public bool KeyIsStatic;
            public string KeyValue;
        }

        private readonly static string[] SessionKeywords = new string[] { "ThreadId", "FileName", "FileNameLine", "SSTABLEPATH=>DDLITEMNAME", "SSTABLEPATH=>KEYSPACE", "SSTABLEPATH=>KEYSPACEDDLNAME", "SSTABLEPATHS=>DDLITEMNAME", "SSTABLEPATHS=>KEYSPACE", "SSTABLEPATHS=>KEYSPACEDDLNAME", "SSTABLEPATH=>TABLEVIEWNAME", "SSTABLEPATH=>KEYSPACETABLEVIEWNAME", "SSTABLEPATHS=>TABLEVIEWNAME", "SSTABLEPATHS=>KEYSPACETABLEVIEWNAME", "ThreadName"};

        readonly CacheInfo[] CachedInfo = new CacheInfo[] { new CacheInfo(), new CacheInfo(), new CacheInfo(), new CacheInfo() };
        private CacheInfo GetCacheInfo(int cacheIdx)
        {
            return CachedInfo[cacheIdx];
        }

        private string CacheInfoVarName(int cacheIdx)
        {
            if (cacheIdx == SessionKeyIdx)
                return "SessionKey";
            else if (cacheIdx == SessionKeyLookupIdx)
                return "SessionKeyLookup";
            else if (cacheIdx == SubClassIdx)
                return "SubClass";
            else if (cacheIdx == DeltaRunningTotalKeyIdx)
                return "DeltaRunningTotalKey";

            return "<Unknown CLogLineTypeParser Property Name>";
        }

        private string CacheInfoVarValue(CLogLineTypeParser clogLineTypePasrer, int cacheIdx)
        {
            if (cacheIdx == SessionKeyIdx)
                return clogLineTypePasrer._sessionKey;
            else if (cacheIdx == SessionKeyLookupIdx)
                return clogLineTypePasrer._sessionKeyLookup;
            else if (cacheIdx == SubClassIdx)
                return clogLineTypePasrer._subclass;
            else if (cacheIdx == DeltaRunningTotalKeyIdx)
                return clogLineTypePasrer._deltaRunningTotalKey;

            return null;
        }

        private sealed class NodeRunningCount
        {
            public NodeRunningCount(NodeIdentifier node)
            {
                this.Node = node;
            }

            public readonly NodeIdentifier Node;
            public long Count = 0;
        }

        private IList<NodeRunningCount> _nodesRunningCnt = null;

        #endregion

        [Flags]
        public enum SessionLookupActions
        {
            Default = 0,
            Label = 0x0001,
            /// <summary>
            /// If defined, the Session Lookup value is used to perform the action (e.g., read, remove, define, etc.) on the Session Key cache (If Label or Stack defined and value not found in session; lookup action cache is used)
            /// </summary>
            Session = 0x0002,
            Read = 0x0004,
            Define = 0x0008,
            Delete = 0x0010,
            /// <summary>
            /// When defining a label instead of creating it, it is pushed onto a stack that upon delete it is popped off.
            /// </summary>
            Stack = 0x0020,
            /// <summary>
            /// If defined the lookup value is used as the primary Session tie-out Id. If not defined and Session Key defined, the session key is used.
            /// </summary>
            TieOutId = 0x0040,
            /// <summary>
            /// If defined the lookup value is appended before the session id (if defined) when a tie-out id is created.
            /// Typically this is done to allow one session end item to match multiple session begins.
            /// </summary>
            AppendTieOutId = 0x0080, 
            ReadSession = Read | Session,
            ReadLabel = Read | Label,
            DefineLabel = Define | Label,
            DeleteLabel = Delete | Label,
            ReadRemoveLabel = Read | Delete | Label,
            ReadRemoveSession = Read | Delete | Session,
            DefineSession = Define | Session,
            DeleteSession = Delete | Session
        }

        public enum SessionKeyActions
        {
            Auto = 0,
            Add = 1,
            Read = 2,
            Delete = 3,
            ReadRemove = 4
        }

        public enum SessionParentActions
        {
            /// <summary>
            /// Current session and it&apos;s parents are added
            /// </summary>
            Default = 0,
            /// <summary>
            /// Current session is not added as a parent. Note that the current session&apos;s parents are added, if any. 
            /// </summary>
            IgnoreCurrent = 1,
            /// <summary>
            /// Current session&apos;s parents are not added
            /// </summary>
            IgnoreParents = 2
        }

        [Flags]
        public enum PropertyInherentOptions
        {
            None = 0,
            /// <summary>
            /// If defined, the property is overwritten regardless if value exists or not, otherwise it is updated only if no value is defined
            /// </summary>
            Overwrite = 0x0001,
            /// <summary>
            /// If defined the values are merged
            /// </summary>
            Merge = 0x0002,
            SubClass = 0x0010,
            PrimaryKS = 0x0020,
            PrimaryDDL = 0x0040,            
            SSTableFilePaths = 0x0080,
            DDLInstances = 0x0100,
            AssocatedNodes = 0x0200,
            TokenRanges = 0x0400,
            /// <summary>
            /// looks for the "tag" log property key.
            /// </summary>
            TagLogProp  = 0x0800,
            OptionsProp = 0x1000,
            All = SubClass | PrimaryDDL | PrimaryKS | SSTableFilePaths | DDLInstances | AssocatedNodes | TokenRanges | TagLogProp | OptionsProp,
            Repair = SubClass | PrimaryKS | DDLInstances | AssocatedNodes | TokenRanges | TagLogProp | OptionsProp
        }

        #region constructors
        public CLogLineTypeParser()
        {
            if (SessionLookupAction == SessionLookupActions.Default)
                this.SessionLookupAction = SessionLookupActions.ReadSession;
        }
               
        #endregion

        #region public properties

        /// <summary>
        /// Used to uniquely identify this parser block.
        /// If a session, the begin session decimal should be zero and the session items should be an increment of the begin session. Example: Begin - 4.0, Item - 4.1.
        /// </summary>
        public decimal TagId;
        public string Description;
        /// <summary>
        /// If a session item this should be the session begin blokck&apos;s TagId.
        /// </summary>
        public decimal? SessionBeginTagId = null;

        /// <summary>
        /// If null any DSE version will match this mappings.
        /// If defined, any DSE version that is equal to or greater than this version will match this mapping.
        /// </summary>
        /// <example>
        /// Match Versions:
        ///     4.7.0
        ///     4.7.5
        ///     4.8.0
        ///     4.8.5
        ///     5.0.0
        /// DSE Version 4.8.12 -&gt; 4.8.5
        ///             4.8.4  -&gt; 4.8.0
        ///             4.8.0  -&gt; 4.8.0
        /// </example>
        public Version MatchVersion { get; set; }
        /// <summary>
        /// RegEx used to match the log line&apos;s level. e.g., Debug, Info, Warn, Error, Fatal
        /// </summary>
        public RegExParseString LevelMatch { get; set; }
        /// <summary>
        /// Log line: INFO  [STREAM-INIT-/10.198.59.26:57796] 2017-01-16 13:34:17,782  StreamResultFuture.java:109 - [Stream #8cf4e000-dc33-11e6-acce-516f8727826c ID#0] Creating new streaming plan for Repair
        /// RegEx used to match the log line&apos;s Thread Id value. e.g.,  &quot;STREAM-IN-/10.198.59.26 &quot;
        /// </summary>
        public RegExParseString ThreadIdMatch { get; set; }
        /// <summary>
        /// Log line: INFO  [STREAM-INIT-/10.198.59.26:57796] 2017-01-16 13:34:17,782  StreamResultFuture.java:109 - [Stream #8cf4e000-dc33-11e6-acce-516f8727826c ID#0] Creating new streaming plan for Repair
        /// RegEx used to match the log line&apos;s file name value. e.g.,  &quot;StreamResultFuture.java &quot;
        /// </summary>
        public RegExParseString FileNameMatch { get; set; }
        /// <summary>
        /// Log line: INFO  [STREAM-INIT-/10.198.59.26:57796] 2017-01-16 13:34:17,782  StreamResultFuture.java:109 - [Stream #8cf4e000-dc33-11e6-acce-516f8727826c ID#0] Creating new streaming plan for Repair
        /// RegEx used to match the log line&apos;s file line position within the file name. e.g., 109
        /// </summary>
        public RegExParseString FileLineMatch { get; set; }
        /// <summary>
        /// Log line: INFO  [STREAM-INIT-/10.198.59.26:57796] 2017-01-16 13:34:17,782  StreamResultFuture.java:109 - [Stream #8cf4e000-dc33-11e6-acce-516f8727826c ID#0] Creating new streaming plan for Repair
        /// RegEx used to match the log line&apos;s message value. e.g., &quot;[Stream #8cf4e000-dc33-11e6-acce-516f8727826c ID#0] Creating new streaming plan for Repair &quot;
        /// </summary>
        public RegExParseString MessageMatch { get; set; }

        /// <summary>
        /// A RegEx that is used splits the log message into different components used to populate the LogCassandraEvent&apos;s LogProperties member property dictionary.
        /// The key is the RegEx named capture group or the capture group position. The dictionary value is the Captured group&apos;s value that is converted to the proper type.
        /// If named capture group is a unit of measure (not a date or time frame), the UOM can be defined by placing type after the name separated by an underscore.
        /// e.g., &quot;(?&lt;mygroupname_mb_sec&gt;.+)&quot; where the property key is &quot;mygroupname&quot; and the group&apos;s UOM type is a megabyte per second.
        /// If separated by two underscores, the separated value should be an already defined property name where that UOM would be used.
        /// e.g., &quot;(?&lt;myothername__mygroupname&gt;.+)&quot; where the property key is &quot;myothername&quot; and the group&apos;s UOM type is based on the property &quot;mygroupname&quot; (i.e., megabyte per second).
        ///
        /// There are special group names. They are:
        ///     &quot;SUBCLASS&quot                                 -- this will map the capture group&apos;s value to property &quot;SubClass&quot; of LogCassandraEvent class
        ///     &quot;ID&quot;                                      -- this will map the capture group&apos;s value to property &quot;Id&quot; of LogCassandraEvent class. It must be a valid GUID.
        ///     &quot;DURATION_xxx&quot; or &quot;DURATION&quot;    -- this will map the capture group&apos;s value to property &quot;Duration&quot; of LogCassandraEvent class
        ///                                                             &quot;xxx&quot; is the time unit. If not provided the time unit must be part of the capture (e.g., &quot;10 ms&quot;)
        ///     &quot;EVENTDURATIONDATETIME&quot;                   -- this will map the capture group&apos;s value to property &quot;EventTimeDuration&quot; and calaculates the &quot;Duration&quot; of the LogCassandraEvent class
        ///                                                             The value must be valid date-time syntax
        ///     &quot;KEYSPACE&quot;                                -- this will map the capture group&apos;s value to property &quot;Keyspace&quot; of the LogCassandraEvent class
        ///                                                             This should be considered the &quot;primary&quot; keyspace for the event.
        ///     &quot;DDLITEMNAME&quot;                             -- this will map the capture group&apos;s value to property &quot;DDLItems&quot; of the LogCassandraEvent class.
        ///                                                             This can be table, view, or index qualifier with optional keyspace qualifier. The separator can be a period or slash.
        ///                                                             This should be considered the &quot;primary&quot; DDL item for the event.
        ///     &quot;TABLEVIEWNAME&quot;                           -- Similar to DDLITEMNAME except that only a table or view name is defined. e.g., tablea.indexa DDLITEMNAME would be &quot;indexa&quot; but TABLEVIEWNAME would be &quot;tablea&quot;
        ///                                                             If TABLEVIEWNAME is not found DDLITEMNAME is used.
        ///     &quot;DDLSCHEMAID&quot;                             -- this will map the capture group&apos;s value to property &quot;DDLItems&quot; of the LogCassandraEvent class, if found.
        ///                                                             This is the GUID of the schema.
        ///                                                             This should be considered the &quot;primary&quot; DDL item for the event.
        ///     &quot;SOLRINDEXNAME&quot                            -- Translate a Solr index name into a quified DDL name. e.g., coafstatim_application_appownrtxt_index => coafstatim.application_appownrtxt_index
        ///     &quot;SSTABLEPATH&quot;                             -- this will be used to obtain the keyspace and DDL item plus map to the capture group&apos;s value to property &quot;SSTables&quot; of the LogCassandraEvent class
        ///                                                             This should be considered the &quot;primary&quot; keyspace and DDL item for the event. This will overide the KEYSPACE and DDLITEMNAME for primary. If this is not the case use SSTABLEPATHS.
        ///     &quot;NODE&quot;                                    -- An IP Adress (can be IP4 or IP6) or a host name.
        ///     &quot;KEYSPACES&quot;                               -- A single or list of keyspaces (see KEYSPACE above)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;DDLITEMNAMES&quot;                            -- A single or list of DDL items (see DDLITEMNAME above)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;SSTABLEPATHS&quot;                            -- A single or list of sstable pathes (see SSTABLEPATHS above)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;NODES&quot;                                   -- A single or a list of nodes (see NODE above)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;TOKENRANGE&quot;                              -- A single pair (e.g., &quot;(8464140199242376099,8468771495322313740]&quot;) or a list of tokens (e.g., &quot;(8464140199242376099,8468771495322313740],(8501968907842620341,8530644509697221859]&quot;)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;TOKENSTART&quot; or &quot;TOKENEND&quot;      -- A single token (e.g., &quot;8464140199242376099&quot;) or a list of tokens (e.g., &quot;8464140199242376099,8468771495322313740,8501968907842620341,8530644509697221859&quot;)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;TOKENSTART&quot; or &quot;TOKENEND&quot;      -- A single token (e.g., &quot;8464140199242376099&quot;) or a list of tokens (e.g., &quot;8464140199242376099,8468771495322313740,8501968907842620341,8530644509697221859&quot;)
        ///                                                             This is either a repeating capture group or a set of capture groups with the same name.
        ///     &quot;KEY&quot; or &quot;VALUE&quot;                -- Defines a Key-Value pair where the key is the key into the LogProperties property of the LogCassandraEvent class.
        ///                                                             The KEY and VALUE capture groups can be anywhere but are processed in order (i.e., fist KEY matches the first VALUE). These can be used in a repeating capture group or a set of capture groups.
        ///     &quot;KEYITEM&quot; and &quot;ITEM&quot;            -- Defines a Key and Value List where the key is the key into the LogProperties property of the LogCassandraEvent class and each ITEM group&apos;s value is inserted into a list.
        ///     &quot;KEYITEM&quot; and &quot;ITEM#&quot;           -- Defines a Key and Value List where the key is the key into the LogProperties property of the LogCassandraEvent class and each ITEM group&apos;s value is inserted into a sublist which is inserted into a list assocated with the key.
        ///                                                             where # is the item number into the sublist.
        ///     &quot;KEYITEM#&quot; and &quot;ITEM#-*&quot;        -- Defines multiple Keys and Value List where the key is the key into the LogProperties property of the LogCassandraEvent class and each ITEM group&apos;s value is inserted into a sublist which is inserted into a list assocated with the key.
        ///                                                             where # is the key number that matches item numbers. * is the item number into the sublist.
        /// </summary>
        /// <remarks>
        /// A collection of values can be defined by using a repeating capture group or a set of capture groups with the same capture group name.
        /// A RegEx examples:
        ///     RegEx: ^Compacting\s+\[(?:SSTableReader\(path=(?&lt;SSTABLES&gt;\&apos;[a-z0-9._@\-/]+\&apos;)\s*\)\s*\,?\s*)+\]$
        ///     Returns a named capture group &quot;SSTABLES&quot; with a list of sstables:
        ///<code>
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459576-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459599-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459573-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459574-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459590-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459587-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459559-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459560-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459558-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459557-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459567-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459568-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459566-Data.db&apos;
        ///         &apos;/data/cassandra/data/usprodofrs/newoffer_3_2_10-70a332308e9a11e6953741bf7da4d7d7/usprodofrs-newoffer_3_2_10-ka-459563-Data.db&apos;
        ///</code>
        ///
        ///     RegEx: ^Compacting\s+\[SSTableReader\(path=(?&lt;SSTABLES&gt;\&apos;[a-z0-9._@\-/]+\&apos;)\s*\)\s*\,\s*SSTableReader\(path=(?&lt;SSTABLES&gt;\&apos;[a-z0-9._@\-/]+\&apos;)\s*\)\s*\,\s*(?:SSTableReader\(path=(?&lt;SSTABLES&gt;\&apos;[a-z0-9._@\-/]+\&apos;)\s*\)\s*\,?\s*)+\]$
        ///     This RegEx string will product the same result as above even though three separate capture groups with the same name (SSTABLE) are defined.
        /// </remarks>
        public RegExParseString ParseMessage { get; set; }
        /// <summary>
        /// A RegEx that is used splits the log threadid into different components used to populate the LogCassandraEvent&apos;s LogProperties member property dictionary.
        /// The key is the RegEx named capture group or the capture group position. The dictionary value is the Captured group&apos;s value that is converted to the proper type.
        /// If named capture group is a unit of measure (not a date or time frame), the UOM can be defined by placing type after the name separated by an underscore.
        /// e.g., &quot;(?&lt;mygroupname_mb_sec&gt;.+)&quot; where the property key is &quot;mygroupname&quot; and the group&apos;s UOM type is a megabyte per second.
        ///
        /// There are special group names. They are:
        /// <see cref="ParseMessage"/>
        /// </summary>
        public RegExParseString ParseThreadId { get; set; }
        public EventTypes EventType { get; set; }
        public EventClasses EventClass { get; set; }

        /// <summary>
        /// If the running count is equal to this value all log events for this instance are ignored. For a Session item this must be defined at the SessionBegin EventType.
        /// </summary>
        public long MaxNumberOfEvents { get; set; } = -1;
        /// <summary>
        /// If the running count for this node is equal to this value all log events for this instance are ignored. For a Session item this must be defined at the SessionBegin EventType.
        /// </summary>
        public long MaxNumberOfEventsPerNode { get; set; } = -1;

        public long RunningCount = 0;

        /// <summary>
        /// If true, the event is processed but ignored as a log event.
        /// </summary>
        public bool IgnoreEvent { get; set; }

        /// <summary>
        /// If disabled (true), the rule is not included in the parsing rules at all. 
        /// This is different from IgnoreEvent where in this case the rule is included but the event is ignored.
        /// </summary>
        public bool Disabled { get; set; } = false;

        /// <summary>
        /// Only applies with Debug builds. If true a debug-break occurs while in the debugger when processing the assocated message.
        /// </summary>
        public bool BrekOnDebug { get; set; } = false;

        /// <summary>
        /// Properties that can be inherent from the parent session
        /// </summary>
        public PropertyInherentOptions PropertyInherentOption { get; set; } = PropertyInherentOptions.None;

        /// <summary>
        /// If true and a session event, all log properties from the prior session events will be merged into this event. Current event&apos;s log properties are not overwritten.
        /// If duplicated log properties are present, the event&apos;s property that is most current is used.
        /// </summary>
        public bool LogPropertySessionMerge { get; set; }

        /// <summary>
        /// If true (default), this event is associated to the this node. If false, the event is not stored (associated) to the node and will not be available in the LogEvents property 
        /// </summary>
        public bool AssociateEventToNode { get; set; } = true;

        private const int SubClassIdx = 1;
        private string _subclass = null;
        /// <summary>
        /// e.g., Hint, Tombstone, etc. or null
        /// This takes the same values as <see cref="SessionKey"/>
        /// Callers should call the <see cref="DetermineSubClass(Cluster, INode, IKeyspace, IDictionary{string, object}, ILogMessage)"/> method to obtain the correct value.
        /// </summary>
        public string SubClass
        {
            get { return this._subclass; }
            set
            {
                if(value != this._subclass)
                {
                    this._subclass = value;
                    CacheSessionKey(this, ref this._subclass, SubClassIdx, true);
                }
            }
        }

        /// <summary>
        /// The associated analytics group used to summarized this log event.
        /// </summary>
        public string AnalyticsGroup { get; set; }

        /// <summary>
        /// The property name (group name) of a running total value that will be converted to a delta between instances.
        /// </summary>
        public string[] DeltaRunningTotalProperty { get; set; }

        private const int DeltaRunningTotalKeyIdx = 3;
        private string _deltaRunningTotalKey = null;
        /// <summary>
        /// If defined it will be the unique key used to access the DeltaRunningTotalProperty running total value.
        /// Callers should call the <see cref="DetermineDeltaRunningTotalKey(Cluster, INode, IKeyspace, IDictionary{string, object}, ILogMessage)"/> method to obtain the correct value.
        /// </summary>
        public string DeltaRunningTotalKey
        {
            get { return this._deltaRunningTotalKey; }
            set
            {
                if (value != this._subclass)
                {
                    this._deltaRunningTotalKey = value;
                    CacheSessionKey(this, ref this._deltaRunningTotalKey, DeltaRunningTotalKeyIdx, true);
                }
            }
        }

        public DSEInfo.InstanceTypes Product { get; set; }

        private const int SessionKeyIdx = 0;
        private string _sessionKey = null;
        /// <summary>
        /// This value is used to &quot;tie&quot; together timespan sessions between log lines/events. For beginning timespan events this is used to generate the Session Key.
        /// For middle/ending timespan events that is used to determine the TimespanBegin LogCassandraEvent instance so that the same event Id and other attributes are shared with the related sessions.
        ///
        /// If a keyword is used, the string value is used to create the session key or determine the LogCassandraEvent instance in the open sessions list. Note that the events must be of the same Event Class.
        ///     Keywords can be:
        ///         ThreadId
        ///         ThreadName
        ///         FileName
        ///         FileNameLine (file name and line number)
        ///         EventClass
        ///         SubClass
        ///         SessionIdKey
        ///         Product
        ///         A capture group name (e.g., SSTABLEPATH) as defined in the <see cref="ParseMessage"/> and <see cref="ParseThreadId"/> properties
        ///         SSTABLEPATH=>DDLITEMNAME -- obtains the DDL Item name from the SSTable Path string
        ///         SSTABLEPATH=>KEYSPACE -- obtains the keyspace name from the SSTable Path string
        ///         SSTABLEPATH=>KEYSPACEDDLNAME -- obtains the keyspace and DDL names from the SSTable Path string
        ///         SSTABLEPATH=>TABLEVIEWNAME -- obtains the table or view name from the SSTable Path string
        ///         SSTABLEPATH=>KEYSPACETABLEVIEWNAME -- obtains the keyspace and table/view name from the SSTable Path string
        /// --or--
        ///     A C# expression that returns a string that represents the session key. This string should be able to be generated for any Timespan sessions and will be used to find the LogCassandraEvent instance when the log line is not a TimespanBegin.
        ///             This session key is item1 in the Tuple in the openTimeSpanSessions local variable.
        ///     This express must start with a &apos;{&apos; and end with a &apos;}&apos;
        ///     It must follow C# syntax. It has the following locally defined variables:
        ///         <code>clogLineTypeParser</code> -- A CLogLineTypeParser instance which is an instance of this information
        ///         <code>cluster</code>            -- A Cluster instance assocated with this log parsing file
        ///         <code>node</code>               -- An INode instance assocated with this log parsing file
        ///         <code>keyspace</code>           -- Current IKeyspace instance assocated with this log line. Could be null.
        ///         <code>logLineProperties</code>  -- A IDictionary&lt;string, object&gt; instance where the key is the capture group&apos;s name and the assocated value
        ///         <code>logLineMessage</code>     -- An ILogMessage instance that consistence of this parsed log line
        /// --or--
        ///     A static value that must be surrounded in quotes.
        /// --or--
        ///     A set of keywords seperated by a &apos;+&apos; where the values are concatenated.
        ///
        /// </summary>
        /// <remarks>
        /// If values are concatenated, the ordering is important. Values that are always present (e.g., ThreadId) should be first and values that may not be present or optional should be later (e.g., DDLITEMNAME).
        ///
        /// Below is an example where the SessionKey would be the &quot;ThreadId&quot; since this is used to identify a compacation timespan session for all log lines.
        /// Log line 1 will define the session key with &quot;CompactionExecutor:734&quot;. The remaining log logs will use the ThreadId to get the beginning event.
        /// <code>
        /// INFO  [CompactionExecutor:734] 2017-02-11 23:58:47,586  CompactionTask.java:141 - Compacting [SSTableReader(path='/data/cassandra/data/usprodsec/session_3-ca531e218ed211e6ab872748a53d9d02/usprodsec-session_3-ka-78735-Data.db'), SSTableReader(path='/data/cassandra/data/usprodsec/session_3-ca531e218ed211e6ab872748a53d9d02/usprodsec-session_3-ka-78732-Data.db'), SSTableReader(path='/data/cassandra/data/usprodsec/session_3-ca531e218ed211e6ab872748a53d9d02/usprodsec-session_3-ka-78734-Data.db'), SSTableReader(path='/data/cassandra/data/usprodsec/session_3-ca531e218ed211e6ab872748a53d9d02/usprodsec-session_3-ka-78733-Data.db')]
        /// INFO  [CompactionExecutor:734] 2017-02-11 23:58:47,668  ColumnFamilyStore.java:905 - Enqueuing flush of compactions_in_progress: 164 (0%) on-heap, 0 (0%) off-heap
        /// INFO  [CompactionExecutor:734] 2017-02-11 23:58:47,715  CompactionTask.java:274 - Compacted 4 sstables to[/ data / cassandra / data / usprodsec / session_3 - ca531e218ed211e6ab872748a53d9d02 / usprodsec - session_3 - ka - 78741,].  3,541 bytes to 1,079 (~30% of original) in 128ms = 0.008039MB/s.  35 total partitions merged to 12.  Partition merge counts were {1:1, 3:10, 4:1, }
        /// </code>
        ///
        /// Below is an example where a capture group could be used (i.e., DDLITEMNAMES/ColumnFamily Name) as the SessionKey.
        /// Log line 1 will define the session key with &quot;product_3_2_0&quot; (i.e., DDLITEMNAMES). The remaining log logs will use the DDLITEMNAMES to get the beginning event.
        /// <code>
        /// INFO  [SlabPoolCleaner] 2017-02-12 00:09:07,183  ColumnFamilyStore.java:1211 - Flushing largest CFS(Keyspace='usprodrp', ColumnFamily='product_3_2_0') to free up room. Used total: 0.20/0.00, live: 0.20/0.00, flushing: 0.00/0.00, this: 0.07/0.07
        /// INFO [SlabPoolCleaner] 2017-02-12 00:09:07,183  ColumnFamilyStore.java:905 - Enqueuing flush of product_3_2_0: 146449687 (7%) on-heap, 0 (0%) off-heap
        /// INFO [MemtableFlushWriter:3768] 2017-02-12 00:09:07,183  Memtable.java:347 - Writing Memtable-product_3_2_0@1706942500(20.930MiB serialized bytes, 856689 ops, 7%/0% of on/off-heap limit)
        /// INFO [MemtableFlushWriter:3768] 2017-02-12 00:09:07,384  Memtable.java:382 - Completed flushing /data/cassandra/data/usprodrp/product_3_2_0-ebf4f370e6a511e6a5513b7f00393f0e/usprodrp-product_3_2_0-tmp-ka-1740-Data.db (3.681MiB) for commitlog position ReplayPosition(segmentId= 1486705770182, position= 19949509)
        /// </code>
        /// </remarks>
        public string SessionKey
        {
            get { return this._sessionKey; }
            set
            {
                if (value != this._sessionKey)
                {
                    this._sessionKey = value;
                    CacheSessionKey(this, ref this._sessionKey, SessionKeyIdx);
                }
            }
        }

        public SessionKeyActions SessionKeyAction { get; set; }

        public bool SessionKeyUsesThreadId()
        {
            return this._sessionKey == null ? false : this._sessionKey.StartsWith("ThreadId");
        }

        private const int SessionKeyLookupIdx = 2;
        private string _sessionKeyLookup = null;
        private Regex _sessionLookupRegEx = null;
        /// <summary>
        /// Alternative Value used to lookup the session.
        /// This takes the same values as <see cref="SessionKey"/>
        /// Callers should call the <see cref="DetermineSessionLookup(Cluster, INode, IKeyspace, IDictionary{string, object}, ILogMessage)"/> method to obtain the correct value.
        /// </summary>
        public string SessionLookup
        {
            get { return this._sessionKeyLookup; }
            set
            {
                if (value != this._sessionKeyLookup)
                {
                    this._sessionKeyLookup = value;
                    CacheSessionKey(this, ref this._sessionKeyLookup, SessionKeyLookupIdx);

                    var cacheInfo = this.GetCacheInfo(SessionKeyLookupIdx);
                    if (cacheInfo.KeyIsStatic && cacheInfo.KeyValue != null && cacheInfo.KeyValue.Contains('#'))
                    {
                        this._sessionLookupRegEx = new Regex(cacheInfo.KeyValue.Replace("#", string.Empty), RegexOptions.Compiled);
                    }
                }
            }
        }

        public SessionLookupActions SessionLookupAction { get; set; }

        /// <summary>
        /// Determines how parent sessions are added to the current instane&apos;s Parent property.
        /// </summary>        
        public SessionParentActions SessionParentAction { get; set; } = SessionParentActions.Default;
        public string[] Examples;

        public CLogLineTypeParser SessionBeginReference { get; internal set; } = null;

        public bool IsClone { get; private set; } = false;

        private decimal? _linkedTag = null;

        /// <summary>
        /// This Tag Id will be used to associated this instance to the matching tag id which must be defined in the master parser collection.
        /// The matching instance values will be copied to this instance. If this instance already has values defined, they will NOT be overwritten.
        /// 
        /// This property should be the first property in the JSON string so that reminding properties can be deigned as overrides.
        /// </summary>
        public decimal? LinkedTagId
        {
            get { return this._linkedTag; }
            set
            {
                if (value.HasValue && (!this._linkedTag.HasValue || value.Value != this._linkedTag))
                {
                    bool forceUpdate = this._linkedTag.HasValue;
                    this._linkedTag = value;

                    var masterParser = LibrarySettings.MasterLog4NetParser();
                    var useParser = masterParser.Parsers.FirstOrDefault(p => p.TagId == this._linkedTag);

                    if(useParser == null)
                    {
                        throw new System.ArgumentOutOfRangeException("MasterLog4NetParser",
                                                                        string.Format("Master CLogLineTypeParser Tag Id {0} was not found for LinkedTagId", this._linkedTag));
                    }

                    this.Clone(useParser, forceUpdate);

                    if(Logger.Instance.IsDebugEnabled)
                    {
                        Logger.Instance.DebugFormat("Using Master CLogLineTypeParser Tag Id of {0} ({1}) for Linked Id of {2} ({3})",
                                                        useParser.TagId,
                                                        useParser.Description,
                                                        this.TagId,
                                                        this.Description);
                    }
                }
                else
                {
                    this._linkedTag = value;
                }
            }
        }
        #endregion

        #region public methods

        public void Clone(CLogLineTypeParser useAsCopy, bool forceUpdate = false)
        {
            if(forceUpdate)
            {
                this.TagId = useAsCopy.TagId;
                this.Description = useAsCopy.Description;
                this.SessionBeginTagId = useAsCopy.SessionBeginTagId;
                this.MatchVersion = useAsCopy.MatchVersion;
                this.LevelMatch = useAsCopy.LevelMatch;
                this.ThreadIdMatch = useAsCopy.ThreadIdMatch;
                this.FileNameMatch = useAsCopy.FileNameMatch;
                this.FileLineMatch = useAsCopy.FileLineMatch;
                this.MessageMatch = useAsCopy.MessageMatch;
                this.ParseMessage = useAsCopy.ParseMessage;
                this.ParseThreadId = useAsCopy.ParseThreadId;
                this.EventType = useAsCopy.EventType;
                this.EventClass = useAsCopy.EventClass;
                this.MaxNumberOfEvents = useAsCopy.MaxNumberOfEvents;
                this.MaxNumberOfEventsPerNode = useAsCopy.MaxNumberOfEventsPerNode;
                this.RunningCount = useAsCopy.RunningCount;
                this.IgnoreEvent = useAsCopy.IgnoreEvent;
                this.Disabled = useAsCopy.Disabled;
                this.PropertyInherentOption = useAsCopy.PropertyInherentOption;
                this.LogPropertySessionMerge = useAsCopy.LogPropertySessionMerge;
                this.AssociateEventToNode = useAsCopy.AssociateEventToNode;
                this._subclass = useAsCopy._subclass;
                this.AnalyticsGroup = useAsCopy.AnalyticsGroup;
                this.DeltaRunningTotalProperty = useAsCopy.DeltaRunningTotalProperty;
                this._deltaRunningTotalKey = useAsCopy._deltaRunningTotalKey;
                this.Product = useAsCopy.Product;
                this._sessionKey = useAsCopy._sessionKey;
                this.SessionKeyAction = useAsCopy.SessionKeyAction;
                this._sessionKeyLookup = useAsCopy._sessionKeyLookup;
                this._sessionLookupRegEx = useAsCopy._sessionLookupRegEx;
                this.SessionLookupAction = useAsCopy.SessionLookupAction;
                this.SessionParentAction = useAsCopy.SessionParentAction;
                this.Examples = useAsCopy.Examples;
                this.SessionBeginReference = useAsCopy.SessionBeginReference;
                                
                this._nodesRunningCnt = useAsCopy._nodesRunningCnt;
                for (int nIdx = 0; nIdx < this.CachedInfo.Length; nIdx++)
                {
                    this.CachedInfo[nIdx] = new CacheInfo()
                    {
                        KeyIsGroupName = useAsCopy.CachedInfo[nIdx].KeyIsGroupName,
                        KeyIsStatic = useAsCopy.CachedInfo[nIdx].KeyIsStatic,
                        KeyMethodInfo = useAsCopy.CachedInfo[nIdx].KeyMethodInfo,
                        KeyMethodInfoIsMethod = useAsCopy.CachedInfo[nIdx].KeyMethodInfoIsMethod,
                        KeyValue = useAsCopy.CachedInfo[nIdx].KeyValue
                    };
                }
            }
            else
            {
                if (this.TagId == 0) this.TagId = useAsCopy.TagId;
                if (this.Description == null) this.Description = useAsCopy.Description;
                if (this.SessionBeginTagId == null) this.SessionBeginTagId = useAsCopy.SessionBeginTagId;
                if (this.MatchVersion == null) this.MatchVersion = useAsCopy.MatchVersion;
                if (this.LevelMatch == null) this.LevelMatch = useAsCopy.LevelMatch;
                if (this.ThreadIdMatch == null) this.ThreadIdMatch = useAsCopy.ThreadIdMatch;
                if (this.FileNameMatch == null) this.FileNameMatch = useAsCopy.FileNameMatch;
                if (this.FileLineMatch == null) this.FileLineMatch = useAsCopy.FileLineMatch;
                if (this.MessageMatch == null) this.MessageMatch = useAsCopy.MessageMatch;
                if (this.ParseMessage == null) this.ParseMessage = useAsCopy.ParseMessage;
                if (this.ParseThreadId == null) this.ParseThreadId = useAsCopy.ParseThreadId;
                if (this.EventType == EventTypes.Unkown) this.EventType = useAsCopy.EventType;
                if (this.EventClass == EventClasses.Unknown) this.EventClass = useAsCopy.EventClass;
                if (this.MaxNumberOfEvents == -1) this.MaxNumberOfEvents = useAsCopy.MaxNumberOfEvents;
                if (this.MaxNumberOfEventsPerNode == -1) this.MaxNumberOfEventsPerNode = useAsCopy.MaxNumberOfEventsPerNode;
                if (this.RunningCount == 0) this.RunningCount = useAsCopy.RunningCount;
                if (!this.IgnoreEvent) this.IgnoreEvent = useAsCopy.IgnoreEvent;
                if (!this.Disabled) this.Disabled = useAsCopy.Disabled;
                if (this.PropertyInherentOption == PropertyInherentOptions.None) this.PropertyInherentOption = useAsCopy.PropertyInherentOption;
                if (!this.LogPropertySessionMerge) this.LogPropertySessionMerge = useAsCopy.LogPropertySessionMerge;
                if (this.AssociateEventToNode) this.AssociateEventToNode = useAsCopy.AssociateEventToNode;
                if (this.SubClass == null) this.SubClass = useAsCopy.SubClass;
                if (this.AnalyticsGroup == null) this.AnalyticsGroup = useAsCopy.AnalyticsGroup;
                if (this.DeltaRunningTotalProperty == null) this.DeltaRunningTotalProperty = useAsCopy.DeltaRunningTotalProperty;
                if (this.DeltaRunningTotalKey == null) this.DeltaRunningTotalKey = useAsCopy.DeltaRunningTotalKey;
                if (this.Product == DSEInfo.InstanceTypes.Unkown) this.Product = useAsCopy.Product;
                if (this.SessionKey == null) this.SessionKey = useAsCopy.SessionKey;
                if (this.SessionKeyAction == SessionKeyActions.Auto) this.SessionKeyAction = useAsCopy.SessionKeyAction;
                if (this.SessionLookup == null) this.SessionLookup = useAsCopy.SessionLookup;
                if (this.SessionLookupAction == SessionLookupActions.Default || this.SessionLookupAction == SessionLookupActions.ReadSession) this.SessionLookupAction = useAsCopy.SessionLookupAction;
                if (this.SessionParentAction == SessionParentActions.Default) this.SessionParentAction = useAsCopy.SessionParentAction;
                if (this.Examples == null) this.Examples = useAsCopy.Examples;
                if (this.SessionBeginReference == null) this.SessionBeginReference = useAsCopy.SessionBeginReference;
                if (this._nodesRunningCnt == null) this._nodesRunningCnt = useAsCopy._nodesRunningCnt;                
            }
            
            this.IsClone = true;
    }

        public struct SessionInfo
        {
            public string SessionId;
            public SessionKeyActions SessionAction;
            public string SessionLookup;
            public SessionLookupActions SessionLookupAction;
            public Regex SessionLookupRegEx;

            public bool IsLookupMatch(string checkValue)
            {
                return this.SessionLookupRegEx == null ? this.SessionLookup == checkValue : this.SessionLookupRegEx.IsMatch(checkValue);
            }

            public Tuple<LogCassandraEvent, string> GetLogEvent(Dictionary<string, LogCassandraEvent> openSessions,
                                                                    Dictionary<string, Stack<LogCassandraEvent>> lookupSessionLabels)
            {
                string sessionKey = null;
                LogCassandraEvent logEvent = null;

                if (this.SessionId != null
                        && (this.SessionAction == SessionKeyActions.Auto
                                || this.SessionAction == SessionKeyActions.Read
                                || this.SessionAction == SessionKeyActions.ReadRemove))
                {
                    logEvent = openSessions.TryGetValue(this.SessionId);
                    
                    if(this.SessionLookup == null)
                    {
                        sessionKey = this.SessionId;
                    }
                    else
                    {
                        if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.AppendTieOutId) == CLogLineTypeParser.SessionLookupActions.AppendTieOutId
                                        && this.SessionId != null)
                        {
                            sessionKey = this.SessionLookup + ',' + this.SessionId;
                        }
                        else if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.TieOutId) == CLogLineTypeParser.SessionLookupActions.TieOutId)
                        {
                            sessionKey = this.SessionLookup;
                        }
                        else
                        {
                            sessionKey = this.SessionId;
                        }
                    }                    
                }

                if (logEvent == null
                        && (this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.Read) == CLogLineTypeParser.SessionLookupActions.Read)
                {
                    if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.Session) == CLogLineTypeParser.SessionLookupActions.Session)
                    {
                        if (this.SessionLookupRegEx == null)
                        {
                            if (this.SessionLookup != null)
                            {
                                logEvent = openSessions.TryGetValue(this.SessionLookup);
                                sessionKey = this.SessionLookup;
                            }
                        }
                        else
                        {
                            var refThis = this;
                            logEvent = openSessions.FirstOrDefault(kv => refThis.IsLookupMatch(sessionKey = kv.Key)).Value;
                            if (logEvent == null) sessionKey = null;
                        }
                    }

                    if (logEvent == null
                            && (this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.Label) == CLogLineTypeParser.SessionLookupActions.Label)
                    {
                        if (this.SessionLookupRegEx == null)
                        {
                            if (this.SessionLookup != null)
                            {
                                logEvent = lookupSessionLabels.TryGetValue(this.SessionLookup);

                               if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.AppendTieOutId) == CLogLineTypeParser.SessionLookupActions.AppendTieOutId
                                        && this.SessionId != null)
                                {
                                    sessionKey = this.SessionLookup + ',' + this.SessionId;
                                }
                                else if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.TieOutId) == CLogLineTypeParser.SessionLookupActions.TieOutId)
                                {
                                    sessionKey = this.SessionLookup;
                                }
                                else
                                {
                                    sessionKey = this.SessionId ?? this.SessionLookup;
                                }
                            }                            
                        }
                        else
                        {
                            string possibleSessionLookup = null;
                            var refThis = this;
                            var stackItem = lookupSessionLabels.FirstOrDefault(kv => refThis.IsLookupMatch(possibleSessionLookup = kv.Key)).Value;
                            logEvent = stackItem == null ? null : (stackItem.Count == 0 ? null : stackItem.Peek());

                            if(possibleSessionLookup == null)
                            {
                                sessionKey = this.SessionId;
                            }
                            else
                            if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.AppendTieOutId) == CLogLineTypeParser.SessionLookupActions.AppendTieOutId
                                        && this.SessionId != null)
                            {
                                sessionKey = this.SessionLookup + ',' + this.SessionId;
                            }
                            else if ((this.SessionLookupAction & CLogLineTypeParser.SessionLookupActions.TieOutId) == CLogLineTypeParser.SessionLookupActions.TieOutId)
                            {
                                sessionKey = possibleSessionLookup;
                            }
                            else
                            {
                                sessionKey = this.SessionId ?? possibleSessionLookup;
                            }
                        }
                    }
                }

                return new Tuple<LogCassandraEvent, string>(logEvent, sessionKey);
            }

            public LogCassandraEvent UpdateSessionStatus(Dictionary<string, LogCassandraEvent> openSessions,
                                                            Dictionary<string, Stack<LogCassandraEvent>> lookupSessionLabels,
                                                            LogCassandraEvent logEvent,
                                                            bool addSessionKey = false)
            {
                if (logEvent == null) return null;
                LogCassandraEvent oldValue = null;

                if (this.SessionId != null)
                {
                    var sessionType = this.SessionAction;

                    if (addSessionKey)
                    {
                        sessionType = SessionKeyActions.Add;
                    }
                    else if (sessionType == SessionKeyActions.Auto)
                    {
                        if ((logEvent.Type & EventTypes.SessionBegin) == EventTypes.SessionBegin)
                        {
                            sessionType = SessionKeyActions.Add;
                        }
                        else if ((logEvent.Type & EventTypes.SessionEnd) == EventTypes.SessionEnd)
                        {
                            sessionType = SessionKeyActions.Delete;
                        }
                    }

                    switch (sessionType)
                    {
                        case SessionKeyActions.Add:
                            oldValue = openSessions.SwapValue(this.SessionId, logEvent);
                            break;
                        case SessionKeyActions.Delete:
                        case SessionKeyActions.ReadRemove:
                            if(!openSessions.Remove(this.SessionId))
                            {
                                var refSessionId = this.SessionId;
                                var delSessionId = openSessions.FirstOrDefault(s => s.Value.Id == logEvent.Id && s.Key.StartsWith(refSessionId)).Key;

                                if(delSessionId != null)
                                {
                                    openSessions.Remove(delSessionId);
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }

                if (this.SessionLookupRegEx == null && this.SessionLookup != null)
                {
                    if ((this.SessionLookupAction & SessionLookupActions.Session) == SessionLookupActions.Session)
                    {
                        if ((this.SessionLookupAction & SessionLookupActions.Define) == SessionLookupActions.Define)
                        {
                            oldValue = openSessions.SwapValue(this.SessionLookup, logEvent);
                        }
                        else if ((this.SessionLookupAction & SessionLookupActions.Delete) == SessionLookupActions.Delete)
                        {
                            if (!openSessions.Remove(this.SessionLookup))
                            {
                                var refSessionId = this.SessionLookup;
                                var delSessionId = openSessions.FirstOrDefault(s => s.Value.Id == logEvent.Id && s.Key.StartsWith(refSessionId)).Key;

                                if (delSessionId != null)
                                {
                                    openSessions.Remove(delSessionId);
                                }
                            }
                        }
                    }

                    if ((this.SessionLookupAction & SessionLookupActions.Label) == SessionLookupActions.Label)
                    {
                        if ((this.SessionLookupAction & SessionLookupActions.Define) == SessionLookupActions.Define)
                        {
                            if ((this.SessionLookupAction & SessionLookupActions.Stack) == SessionLookupActions.Stack)
                            {
                                lookupSessionLabels.TryAddAppendCollection(this.SessionLookup, logEvent);
                            }
                            else
                            {
                                lookupSessionLabels.TryAddOrUpdateCollection(this.SessionLookup, logEvent);
                            }
                        }
                        else if ((this.SessionLookupAction & SessionLookupActions.Delete) == SessionLookupActions.Delete)
                        {
                            lookupSessionLabels.TryRemoveCollection(this.SessionLookup);
                        }
                    }
                }
                else if (this.SessionLookupRegEx != null
                            && (this.SessionLookupAction & SessionLookupActions.Delete) == SessionLookupActions.Delete)
                {
                    if ((this.SessionLookupAction & SessionLookupActions.Label) == SessionLookupActions.Label)
                    {
                        var refThis = this;
                        lookupSessionLabels.Where(kv => refThis.IsLookupMatch(kv.Key))
                                .Select(kv => kv.Key)
                                .ForEach(k => lookupSessionLabels.Remove(k));
                    }
                    if ((this.SessionLookupAction & SessionLookupActions.Session) == SessionLookupActions.Session)
                    {
                        var refThis = this;
                        openSessions.Where(kv => refThis.IsLookupMatch(kv.Key))
                                .Select(kv => kv.Key)
                                .ForEach(k => openSessions.Remove(k));
                    }
                }

                return oldValue;
            }
        }

        public SessionInfo DetermineSessionInfo(Cluster cluster,
                                                    INode node,
                                                    IKeyspace keyspace,
                                                    IDictionary<string, object> logLineProperties,
                                                    ILogMessage logLineMessage)
        {
            SessionInfo sessionInfo;

            sessionInfo.SessionAction = this.SessionKeyAction;
            sessionInfo.SessionLookupAction = this.SessionLookupAction;

            sessionInfo.SessionId = this.DetermineSessionKey(cluster, node, keyspace, logLineProperties, logLineMessage);

            if (this._sessionLookupRegEx == null)
            {
                sessionInfo.SessionLookup = this.DetermineSessionLookup(cluster, node, keyspace, logLineProperties, logLineMessage);

                if (sessionInfo.SessionLookup != null && sessionInfo.SessionLookup.Contains('#'))
                {
                    sessionInfo.SessionLookupRegEx = new Regex(sessionInfo.SessionLookup.Replace("#", string.Empty));
                    sessionInfo.SessionLookup = null;
                }
                else
                {
                    sessionInfo.SessionLookupRegEx = null;
                }
            }
            else
            {
                sessionInfo.SessionLookupRegEx = this._sessionLookupRegEx;
                sessionInfo.SessionLookup = null;
            }

            return sessionInfo;
        }

        public string DetermineSessionKey(Cluster cluster,
                                            INode node,
                                            IKeyspace keyspace,
                                            IDictionary<string, object> logLineProperties,
                                            ILogMessage logLineMessage)
        {
            return this._sessionKey == null
                    ? null
                    : DetermineKeyValue(this,
                                        SessionKeyIdx,
                                        cluster,
                                        node,
                                        keyspace,
                                        logLineProperties,
                                        logLineMessage);
        }

        public string DetermineSubClass(Cluster cluster,
                                        INode node,
                                        IKeyspace keyspace,
                                        IDictionary<string, object> logLineProperties,
                                        ILogMessage logLineMessage)
        {
            return this._subclass == null
                    ? null
                    : DetermineKeyValue(this,
                                        SubClassIdx,
                                        cluster,
                                        node,
                                        keyspace,
                                        logLineProperties,
                                        logLineMessage,
                                        false,
                                        true);
        }

        public string DetermineDeltaRunningTotalKey(Cluster cluster,
                                                        INode node,
                                                        IKeyspace keyspace,
                                                        IDictionary<string, object> logLineProperties,
                                                        ILogMessage logLineMessage)
        {
            return this._deltaRunningTotalKey == null
                    ? null
                    : DetermineKeyValue(this,
                                        DeltaRunningTotalKeyIdx,
                                        cluster,
                                        node,
                                        keyspace,
                                        logLineProperties,
                                        logLineMessage,
                                        false,
                                        true);
        }

        public string DetermineSessionLookup(Cluster cluster,
                                                INode node,
                                                IKeyspace keyspace,
                                                IDictionary<string, object> logLineProperties,
                                                ILogMessage logLineMessage)
        {
            return this._sessionKeyLookup == null
                    ? null
                    : DetermineKeyValue(this,
                                        SessionKeyLookupIdx,
                                        cluster,
                                        node,
                                        keyspace,
                                        logLineProperties,
                                        logLineMessage);
        }

        public bool IncrementRunningCount(INode node)
        {
            var runningCnt = System.Threading.Interlocked.Increment(ref this.RunningCount);

            if (this.MaxNumberOfEvents >= 0)
            {
                return runningCnt >= this.MaxNumberOfEvents;
            }

            if(this.MaxNumberOfEventsPerNode >= 0)
            {
                NodeRunningCount nodeCnt;

                if (this._nodesRunningCnt == null)
                {
                    nodeCnt = new NodeRunningCount(node.Id);
                    this._nodesRunningCnt = new List<NodeRunningCount>() { nodeCnt };
                }
                else
                {
                    nodeCnt = this._nodesRunningCnt.FirstOrDefault(i => i.Node.Equals(node.Id));

                    if(nodeCnt == null) nodeCnt = new NodeRunningCount(node.Id);
                }


                return System.Threading.Interlocked.Increment(ref nodeCnt.Count) >= this.MaxNumberOfEventsPerNode;
            }

            if (this.SessionBeginReference != null)
            {
                return this.SessionBeginReference.CheckRunningCount(node);
            }

            return false;
        }

        public bool CheckRunningCount(INode node)
        {
            var runningCnt = System.Threading.Interlocked.Increment(ref this.RunningCount);

            if (this.MaxNumberOfEvents >= 0)
            {
                return runningCnt >= this.MaxNumberOfEvents;
            }

            if (this.MaxNumberOfEventsPerNode >= 0)
            {
#pragma warning disable IDE0031 // Use null propagation
                NodeRunningCount nodeCnt = this._nodesRunningCnt == null ? null : this._nodesRunningCnt.FirstOrDefault(i => i.Node.Equals(node.Id));
#pragma warning restore IDE0031 // Use null propagation

                return nodeCnt == null ? false : System.Threading.Interlocked.Increment(ref nodeCnt.Count) >= this.MaxNumberOfEventsPerNode;
            }

            if (this.SessionBeginReference != null) return this.SessionBeginReference.CheckRunningCount(node);

            return false;
        }

        public static string GenerateSessionKey(string varName,
                                                    bool isGroupName,
                                                    CLogLineTypeParser logLineParser,
                                                    int cacheIdx,
                                                    Cluster cluster,
                                                    INode node,
                                                    IKeyspace keyspace,
                                                    IDictionary<string, object> logLineProperties,
                                                    ILogMessage logLineMessage)
        {
            object objSessionKey = null;

            if (isGroupName)
            {
                if (!logLineProperties.TryGetValue(varName, out objSessionKey))
                {
                    if (varName == "TABLEVIEWNAME")
                    {
                        logLineProperties.TryGetValue("DDLITEMNAME", out objSessionKey);
                    }
                }
            }
            else
            {
                switch (varName)
                {
                    case "ThreadId":
                        objSessionKey = logLineMessage.ThreadId;
                        break;
                    case "ThreadName": //ValidationExecutor:800
                        {
                            var threadId = logLineMessage.ThreadId;
                            var lstSepPos = threadId.LastIndexOf(':');
                            objSessionKey = lstSepPos > 0 ? threadId.Substring(0, lstSepPos) : threadId;
                        }
                        break;
                    case "FileName":
                        objSessionKey = logLineMessage.FileName;
                        break;
                    case "EventClass":
                        objSessionKey = logLineParser.EventClass.ToString();
                        break;
                    case "Product":
                        objSessionKey = logLineParser.Product.ToString();
                        break;
                    case "FileNameLine":
                        objSessionKey = logLineMessage.FileName + ":" + logLineMessage.FileLine.ToString();
                        break;
                    case "SSTABLEPATH=>DDLITEMNAME":
                    case "SSTABLEPATHS=>DDLITEMNAME":
                        objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, false, false);
                        break;
                    case "SSTABLEPATH=>KEYSPACE":
                    case "SSTABLEPATHS=>KEYSPACE":
                        objSessionKey = DetermineNameFromSSTablePath(logLineProperties, true, false, false);
                        break;
                    case "SSTABLEPATH=>KEYSPACEDDLNAME":
                    case "SSTABLEPATHS=>KEYSPACEDDLNAME":
                        objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, true, false);
                        break;
                    case "SSTABLEPATH=>TABLEVIEWNAME":
                    case "SSTABLEPATHS=>TABLEVIEWNAME":
                        objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, false, true);
                        break;
                    case "SSTABLEPATH=>KEYSPACETABLEVIEWNAME":
                    case "SSTABLEPATHS=>KEYSPACETABLEVIEWNAME":
                        objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, true, true);
                        break;
                    default:
                        {
                            if (cacheIdx != SubClassIdx
                                        && varName == logLineParser.CacheInfoVarName(SubClassIdx))
                            {
                                objSessionKey = logLineParser.DetermineSubClass(cluster,
                                                                                node,
                                                                                keyspace,
                                                                                logLineProperties,
                                                                                logLineMessage);
                            }
                            else if (cacheIdx != SessionKeyIdx
                                        && varName == logLineParser.CacheInfoVarName(SessionKeyIdx))
                            {
                                objSessionKey = logLineParser.DetermineSessionKey(cluster,
                                                                                    node,
                                                                                    keyspace,
                                                                                    logLineProperties,
                                                                                    logLineMessage);
                            }
                            else if (cacheIdx != SessionKeyLookupIdx
                                        && varName == logLineParser.CacheInfoVarName(SessionKeyLookupIdx))
                            {
                                objSessionKey = logLineParser.DetermineSessionLookup(cluster,
                                                                                    node,
                                                                                    keyspace,
                                                                                    logLineProperties,
                                                                                    logLineMessage);
                            }
                            else
                            {
                                objSessionKey = varName;
                            }
                        }
                        break;
                }
            }

            if (objSessionKey != null)
            {
                if (!(objSessionKey is string) && objSessionKey is System.Collections.IEnumerable)
                {
                    return ((IEnumerable<object>)objSessionKey).Sum(s => (long) s.GetHashCode()).ToString();
                }
                else
                {
                    return objSessionKey.ToString();
                }
            }

            return string.Empty;
        }

        #endregion

        #region override

        private string _toStringCache = null;
        public override string ToString()
        {
#pragma warning disable IDE0029 // Use coalesce expression
            return this._toStringCache == null ? this._toStringCache = Newtonsoft.Json.JsonConvert.SerializeObject(this) : this._toStringCache;
#pragma warning restore IDE0029 // Use coalesce expression
        }

        private string _toShortStringCache = null;
        public string ToShortString()
        {
            if (this._toShortStringCache == null)
                this._toShortStringCache = string.Format("{{{{\"TagId\":{0},\"Description\":\"{1}\",\"SessionBeginTagId\":{2},\"EventType\":{3},\"EventClass\":{4},\"RunningCount\":{{0}},\"IsClone\":{5},\"LinkedTagId\":{6},\"IgnoreEvent\":{7} }}}}",
                                                                this.TagId,
                                                                this.Disabled ? this.Description + " (DISABLED)" : this.Description,
                                                                this.SessionBeginTagId,
                                                                this.EventType,
                                                                this.EventClass,
                                                                this.IsClone,
                                                                this.LinkedTagId,
                                                                this.IgnoreEvent);

            return string.Format(this._toShortStringCache, this.RunningCount);
        }

        #endregion

        #region Private Methods
        private static void CacheSessionKey(CLogLineTypeParser logLineParser,
                                            ref string propValue,
                                            int cacheIdx,
                                            bool forceUpdate = false)
        {
            var cacheInfo = logLineParser.GetCacheInfo(cacheIdx);

            if (logLineParser.EventType == EventTypes.SingleInstance && !forceUpdate)
            {
                cacheInfo.KeyMethodInfo = null;
                cacheInfo.KeyMethodInfoIsMethod = false;
                cacheInfo.KeyValue = null;
                cacheInfo.KeyIsGroupName = false;
                cacheInfo.KeyIsStatic = false;
                return;
            }

            propValue = propValue?.Trim();

            if (string.IsNullOrEmpty(propValue))
            {
                cacheInfo.KeyMethodInfo = null;
                cacheInfo.KeyMethodInfoIsMethod = false;
                cacheInfo.KeyValue = null;
                cacheInfo.KeyIsGroupName = true;
                cacheInfo.KeyIsStatic = false;
                return;
            }

            if(propValue == logLineParser.CacheInfoVarName(cacheIdx))
            {
                cacheInfo.KeyMethodInfo = typeof(CLogLineTypeParser).GetMethod(string.Format("Determine{0}", logLineParser.CacheInfoVarName(cacheIdx)),
                                                                                    new Type[]
                                                                                    {
                                                                                        typeof(Cluster),
                                                                                        typeof(INode),
                                                                                        typeof(IKeyspace),
                                                                                        typeof(IDictionary<string,object>),
                                                                                        typeof(ILogMessage)
                                                                                    });
                cacheInfo.KeyMethodInfoIsMethod = true;
                cacheInfo.KeyValue = null;
                cacheInfo.KeyIsGroupName = true;
                cacheInfo.KeyIsStatic = false;
                return;
            }

            if (propValue.IndexOf('+') > 0)
            {
                var keywords = Common.StringFunctions.Split(propValue, '+')
                                    .Select(i =>
                                    {
                                        i = i.Trim();

                                        if (i[0] == '"' && i.Last() == '"')
                                            return i;
                                        if (i[0] == '\'' && i.Last() == '\'')
                                            return '"' + i.Substring(1, i.Length - 2) + '"';
                                        if (i[0] == '\'')
                                            return '"' + i.Substring(1, i.Length - 1) + '"';
                                        if (i.Last() == '\'')
                                            return '"' + i.Substring(0, i.Length - 1) + '"';
                                        if (i[0] == '"' || i.Last() == '"' || i[0] == '\'' || i.Last() == '\'')
                                            throw new ArgumentException(string.Format("Invalid parameter value of \"{0}\" found. Improper quotes found.", i), "propertyValue");
                                        if (i == "EventClass")
                                            return '"' + logLineParser.EventClass.ToString() + '"';
                                        if (i == "EventType")
                                            return '"' + logLineParser.EventType.ToString() + '"';
                                        if (i == "Product")
                                            return '"' + logLineParser.Product.ToString() + '"';
                                        return string.Format("CLogLineTypeParser.GenerateSessionKey(\"{0}\", " +
                                                                                                    "{1}, " +
                                                                                                    "clogLineTypeParser, " +
                                                                                                    "{2}, " +
                                                                                                    "cluster, " +
                                                                                                    "node, " +
                                                                                                    "keyspace, " +
                                                                                                    "logLineProperties, " +
                                                                                                    "logLineMessage)",
                                                                                                    i,
                                                                                                    (!SessionKeywords.Contains(i)).ToString().ToLower(),
                                                                                                    cacheIdx);
                                    });
                cacheInfo.KeyMethodInfo = MiscHelpers.CompileMethod(GenerateMethodName(logLineParser.Product,
                                                                                        logLineParser.EventClass,
                                                                                        logLineParser.EventType,
                                                                                        logLineParser.CacheInfoVarName(cacheIdx),
                                                                                        true),
                                                                                    typeof(string),
                                                                                    new Tuple<Type, string>[]
                                                                                    {
                                                                                        new Tuple<Type, string>(typeof(CLogLineTypeParser), "clogLineTypeParser"),
                                                                                        new Tuple<Type,string>(typeof(Cluster), "cluster"),
                                                                                        new Tuple<Type,string>(typeof(INode), "node"),
                                                                                        new Tuple<Type,string>(typeof(IKeyspace), "keyspace"),
                                                                                        new Tuple<Type,string>(typeof(IDictionary<string,object>), "logLineProperties"),
                                                                                        new Tuple<Type,string>(typeof(ILogMessage), "logLineMessage")
                                                                                    },
                                                                                    string.Format("return {0};",
                                                                                                    string.Join(" + ", keywords)));
                cacheInfo.KeyValue = null;
                cacheInfo.KeyMethodInfoIsMethod = false;
                cacheInfo.KeyIsGroupName = false;
                cacheInfo.KeyIsStatic = false;
                return;
            }

            if (propValue[0] == '"' && propValue.Last() == '"')
            {
                cacheInfo.KeyIsStatic = true;
                cacheInfo.KeyValue = propValue.Substring(1, propValue.Length - 2);
            }
            else if (propValue[0] == '\'' && propValue.Last() == '\'')
            {
                cacheInfo.KeyIsStatic = true;
                cacheInfo.KeyValue = propValue.Substring(1, propValue.Length - 2);
            }
            else if (propValue[0] == '\'')
            {
                cacheInfo.KeyIsStatic = true;
                cacheInfo.KeyValue = propValue.Substring(0, propValue.Length - 1);
            }
            else if (propValue.Last() == '\'')
            {
                cacheInfo.KeyIsStatic = true;
                cacheInfo.KeyValue = propValue.Substring(1, propValue.Length - 1);
            }
            else if (propValue[0] == '{')
            {
                cacheInfo.KeyMethodInfoIsMethod = false;
                cacheInfo.KeyMethodInfo = MiscHelpers.CompileMethod(GenerateMethodName(logLineParser.Product,
                                                                                        logLineParser.EventClass,
                                                                                        logLineParser.EventType,
                                                                                        logLineParser.CacheInfoVarName(cacheIdx),
                                                                                        false),
                                                                                    typeof(string),
                                                                                    new Tuple<Type, string>[]
                                                                                    {
                                                                                        new Tuple<Type, string>(typeof(CLogLineTypeParser), "clogLineTypeParser"),
                                                                                        new Tuple<Type,string>(typeof(Cluster), "cluster"),
                                                                                        new Tuple<Type,string>(typeof(INode), "node"),
                                                                                        new Tuple<Type,string>(typeof(IKeyspace), "keyspace"),
                                                                                        new Tuple<Type,string>(typeof(IDictionary<string,object>), "logLineProperties"),
                                                                                        new Tuple<Type,string>(typeof(ILogMessage), "logLineMessage")
                                                                                    },
                                                                                    propValue);
            }
            else
            {
                cacheInfo.KeyValue = propValue;
                cacheInfo.KeyIsGroupName = !SessionKeywords.Contains(propValue);
            }
        }

        private static string GenerateMethodName(DSEInfo.InstanceTypes instanceType, EventClasses eventClass, EventTypes eventType, string varName, bool getMethod)
        {
            return string.Format("{0}_{1}_{2}_{4}{3}",
                                    string.Join("_", Enum.GetValues(typeof(DSEInfo.InstanceTypes))
                                                .Cast<int>()
                                                .Where(f => f != 0 && (f & ((int)instanceType)) == f)
                                                .Cast<DSEInfo.InstanceTypes>()),
                                    string.Join("_", Enum.GetValues(typeof(EventClasses))
                                                .Cast<long>()
                                                .Where(f => f != 0 && (f & ((long)eventClass)) == f)
                                                 .Cast<EventClasses>()),
                                    string.Join("_", Enum.GetValues(typeof(EventTypes))
                                                .Cast<int>()
                                                .Where(f => f != 0 && (f & ((int)eventType)) == f)
                                                 .Cast<EventTypes>()),
                                    varName,
                                    getMethod ? "Get" : "Determine");
        }

        private static bool GenerateSessionKey(CLogLineTypeParser logLineParser,
                                                    int cacheIdx,
                                                    Cluster cluster,
                                                    INode node,
                                                    IKeyspace keyspace,
                                                    IDictionary<string, object> logLineProperties,
                                                    ILogMessage logLineMessage,
                                                    out string sessionKey)
        {
            var cacheInfo = logLineParser.GetCacheInfo(cacheIdx);

            if (cacheInfo.KeyValue != null)
            {
                object objSessionKey = null;

                if(cacheInfo.KeyIsStatic)
                {
                    sessionKey = cacheInfo.KeyValue;
                    return true;
                }
                else if (cacheInfo.KeyIsGroupName)
                {
                    if (!logLineProperties.TryGetValue(cacheInfo.KeyValue, out objSessionKey))
                    {
                        if (cacheInfo.KeyValue == "TABLEVIEWNAME")
                        {
                            logLineProperties.TryGetValue("DDLITEMNAME", out objSessionKey);
                        }
                    }
                }
                else
                {
                    switch (cacheInfo.KeyValue)
                    {
                        case "ThreadId":
                            objSessionKey = logLineMessage.ThreadId;
                            break;
                        case "ThreadName": //ValidationExecutor:800
                            {
                                var threadId = logLineMessage.ThreadId;
                                var lstSepPos = threadId.LastIndexOf(':');
                                objSessionKey = lstSepPos > 0 ? threadId.Substring(0, lstSepPos) : threadId;
                            }
                            break;
                        case "FileName":
                            objSessionKey = logLineMessage.FileName;
                            break;
                        case "EventClass":
                            objSessionKey = logLineParser.EventClass.ToString();
                            break;
                        case "Product":
                            objSessionKey = logLineParser.Product.ToString();
                            break;
                        case "FileNameLine":
                            objSessionKey = logLineMessage.FileName + ":" + logLineMessage.FileLine.ToString();
                            break;
                        case "SSTABLEPATH=>DDLITEMNAME":
                        case "SSTABLEPATHS=>DDLITEMNAME":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, false, false);
                            break;
                        case "SSTABLEPATH=>KEYSPACE":
                        case "SSTABLEPATHS=>KEYSPACE":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, true, false, false);
                            break;
                        case "SSTABLEPATH=>KEYSPACEDDLNAME":
                        case "SSTABLEPATHS=>KEYSPACEDDLNAME":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, true, false);
                            break;
                        case "SSTABLEPATH=>TABLEVIEWNAME":
                        case "SSTABLEPATHS=>TABLEVIEWNAME":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, false, true);
                            break;
                        case "SSTABLEPATH=>KEYSPACETABLEVIEWNAME":
                        case "SSTABLEPATHS=>KEYSPACETABLEVIEWNAME":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false, true, true);
                            break;
                        default:
                            {
                                if (cacheIdx != SubClassIdx
                                        && cacheInfo.KeyValue == logLineParser.CacheInfoVarName(SubClassIdx))
                                {
                                    objSessionKey = logLineParser.DetermineSubClass(cluster,
                                                                                    node,
                                                                                    keyspace,
                                                                                    logLineProperties,
                                                                                    logLineMessage);
                                }
                                else if (cacheIdx != SessionKeyIdx
                                            && cacheInfo.KeyValue == logLineParser.CacheInfoVarName(SessionKeyIdx))
                                {
                                    objSessionKey = logLineParser.DetermineSessionKey(cluster,
                                                                                        node,
                                                                                        keyspace,
                                                                                        logLineProperties,
                                                                                        logLineMessage);
                                }
                                else if (cacheIdx != SessionKeyLookupIdx
                                            && cacheInfo.KeyValue == logLineParser.CacheInfoVarName(SessionKeyLookupIdx))
                                {
                                    objSessionKey = logLineParser.DetermineSessionLookup(cluster,
                                                                                        node,
                                                                                        keyspace,
                                                                                        logLineProperties,
                                                                                        logLineMessage);
                                }
                            }
                            break;
                    }
                }

                if (objSessionKey != null)
                {
                    if (!(objSessionKey is string) && objSessionKey is System.Collections.IEnumerable)
                    {
                        sessionKey = ((IEnumerable<object>)objSessionKey).Sum(s => s.GetHashCode()).ToString();
                        return false;
                    }
                    else
                    {
                        sessionKey = objSessionKey.ToString();
                    }

                    return true;
                }
            }

            sessionKey = null;
            return false;
        }


        private static string DetermineKeyValue(CLogLineTypeParser logLineParser,
                                                int cacheIdx,
                                                Cluster cluster,
                                                INode node,
                                                IKeyspace keyspace,
                                                IDictionary<string, object> logLineProperties,
                                                ILogMessage logLineMessage,
                                                bool throwWhenKeyNotFnd = true,
                                                bool forceLookup = false)
        {
            if (logLineParser.EventType == EventTypes.SingleInstance
                    && !forceLookup)
            {
                return null;
            }

            var cacheInfo = logLineParser.GetCacheInfo(cacheIdx);

            if(cacheInfo.KeyIsStatic)
            {
                return cacheInfo.KeyValue;
            }
            else if (cacheInfo.KeyMethodInfo != null)
            {
                return cacheInfo.KeyMethodInfoIsMethod
                        ? cacheInfo.KeyMethodInfo.Invoke(logLineParser, new object[] { cluster,
                                                                                        node,
                                                                                        keyspace,
                                                                                        logLineProperties,
                                                                                        logLineMessage }) as string
                        : cacheInfo.KeyMethodInfo.Invoke(null, new object[] { logLineParser,
                                                                                cluster,
                                                                                node,
                                                                                keyspace,
                                                                                logLineProperties,
                                                                                logLineMessage }) as string;
            }
            else
            {               
                if (!GenerateSessionKey(logLineParser,
                                        cacheIdx,
                                        cluster,
                                        node,
                                        keyspace,
                                        logLineProperties,
                                        logLineMessage,
                                        out string sessionKey))
                {
                    if (sessionKey == null && throwWhenKeyNotFnd)
                    {
                        throw new ArgumentException(string.Format("{0} \"{1}\" was not valid for \"{2}\".",
                                                                    logLineParser.CacheInfoVarName(cacheIdx),
                                                                    logLineParser.CacheInfoVarValue(logLineParser, cacheIdx),
                                                                    logLineParser.Examples?.Where(e => e.StartsWith("=>")).FirstOrDefault()),
                                                                    logLineParser.CacheInfoVarName(cacheIdx));
                    }
                }

                return sessionKey;
            }
        }
        private static string DetermineNameFromSSTablePath(IDictionary<string, object> logLineProperties,
                                                            bool returnKeyspaceNameOnly,
                                                            bool returnFullName,
                                                            bool onlyTableName)
        {
#pragma warning disable IDE0018 // Inline variable declaration
            object objSessionKey = null;
#pragma warning restore IDE0018 // Inline variable declaration
            string name = null;

            if(!logLineProperties.TryGetValue("SSTABLEPATH", out objSessionKey))
            {
                logLineProperties.TryGetValue("SSTABLEPATHS", out objSessionKey);
            }

            if (objSessionKey != null)
            {
                string path = null;

                if (objSessionKey is string)
                {
                    path = (string)objSessionKey;
                }
                else if (objSessionKey is System.Collections.IEnumerable)
                {
                    path = ((IEnumerable<object>)objSessionKey).FirstOrDefault()?.ToString();
                }
                else
                {
                    path = objSessionKey.ToString();
                }

                var ksddlnameItems = StringHelpers.ParseSSTableFileIntoKSTableNames(path);

                if (ksddlnameItems != null)
                {
                    var ddlName = onlyTableName ? (ksddlnameItems.Item4 ?? ksddlnameItems.Item2) : ksddlnameItems.Item2;

                    if(returnFullName)
                    {
                        name = (ksddlnameItems.Item1 ?? string.Empty) + '.' + (ddlName ?? string.Empty);
                    }
                    else if (returnKeyspaceNameOnly)
                        name = ksddlnameItems.Item1;
                    else
                        name = ddlName;
                }
            }
            return name;
        }

        #endregion
    }

    public sealed class CLogTypeParser
    {
        private bool _initializeParsers = false;

        public CLogTypeParser() { }

        public CLogTypeParser(string logClass, params CLogLineTypeParser[] args)
        {
            this.LogClass = logClass;
            this.Parsers = args;
        }

        /// <summary>
        /// The name of the class used to parse the Cassandra log file. e.g., file_cassandra_log4net
        /// </summary>
        public string LogClass { get; set; }

        private CLogLineTypeParser[] _parsers;

        public CLogLineTypeParser[] Parsers
        {
            get {  return this._parsers; }
            set
            {
                this._parsers = value == null ? value : value.Where(n => !n.Disabled).ToArray();
            }
        }

        public CLogLineTypeParser[] DetermineParsers(Version dseVersion)
        {
            if(!this._initializeParsers)
            {
                this._initializeParsers = true;

                this.Parsers.ForEach(parser =>
                {
                    if(parser.SessionBeginTagId.HasValue && parser.SessionBeginReference == null)
                    {
                        parser.SessionBeginReference = this.Parsers.FirstOrDefault(i => i.TagId == parser.SessionBeginTagId);
                    }
                });
            }

            var parserList = this.Parsers.Where(m => !m.Disabled && m.MatchVersion == null).ToList();

            if (dseVersion != null)
            {
                var versionedMapper = this.Parsers.Where(m => !m.Disabled && m.MatchVersion != null && dseVersion >= m.MatchVersion)
                                            .OrderByDescending(m => m.MatchVersion)
                                            .DuplicatesRemoved(m => m.EventClass);
                parserList.AddRange(versionedMapper);
            }

            return parserList.ToArray();
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="parsers"></param>
        /// <param name="logEvent"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="onlyMatch"></param>
        /// <returns>
        /// Returns null to indicate that the log event did not match anything.
        /// Returns a tuple where the
        ///     first item is the regex for the thread id or null,
        ///     second item is the match for the thread id or null,
        ///     third item is the regex for the message or null,
        ///     fourth item is the match for the message or null,
        ///     fifth item is the parser instance.
        ///     first and third will never both be null.
        /// </returns>
        public static Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> FindMatch (CLogLineTypeParser[] parsers, 
                                                                                        ILogMessage logEvent,
                                                                                        CancellationToken cancellationToken,
                                                                                        bool onlyMatch = false)
        {
            Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> returnValue = null;
            //var parallelOptions = new ParallelOptions();
            //parallelOptions.CancellationToken = cancellationToken;

            //Parallel.ForEach(parsers, parallelOptions, (parserItem, loopState) =>
            foreach (var parserItem in parsers)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if ((parserItem.FileLineMatch?.IsMatchAny(logEvent.FileLine.ToString()) ?? true)
                        && (parserItem.FileNameMatch?.IsMatchAny(logEvent.FileName) ?? true)
                        && (parserItem.LevelMatch?.IsMatchAny(logEvent.Level.ToString()) ?? true)
                        && (parserItem.ThreadIdMatch?.IsMatchAny(logEvent.ThreadId) ?? true)
                        && (parserItem.MessageMatch?.IsMatchAny(logEvent.Message) ?? true))
                {
                    Regex threadIdRegEx = null;
                    Regex messageRegEx = null;
                    var threadIdMatch = onlyMatch ? null : parserItem.ParseThreadId?.MatchFirstSuccessful(logEvent.ThreadId, out threadIdRegEx);
                    var messageMatch = onlyMatch ? null : parserItem.ParseMessage?.MatchFirstSuccessful(logEvent.Message, out messageRegEx);

                    //if(loopState.IsStopped)
                    //{
                    //    return;
                    //}
                    //else
                    if (onlyMatch || threadIdMatch != null || messageMatch != null)
                    {
                        //if(returnValue == null)
                            returnValue = new Tuple<Regex, Match, Regex, Match, CLogLineTypeParser>(threadIdRegEx,
                                                                                                    threadIdMatch,
                                                                                                    messageRegEx,
                                                                                                    messageMatch,
                                                                                                    parserItem);
                        //loopState.Stop();
                        //return;
                        break;
                    }
                    else
                    {
                        Logger.Instance.WarnFormat("Parsing of Log line \"{0}\" was matched but parsing failed for \"{1}\". Log Line parsing for this line will be skipped.",
                                                     logEvent.ToString(),
                                                     parserItem.ToShortString());                        
                    }
                }               
            }//);

            return returnValue;
        }
    }
}

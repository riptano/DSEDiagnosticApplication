using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Common;

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

        private readonly static string[] SessionKeywords = new string[] { "ThreadId", "FileName", "FileNameLine", "SSTABLEPATH=>DDLITEMNAME", "SSTABLEPATH=>KEYSPACE", "SSTABLEPATH=>KEYSPACEDDLNAME", "SSTABLEPATHS=>DDLITEMNAME", "SSTABLEPATHS=>KEYSPACE", "SSTABLEPATHS=>KEYSPACEDDLNAME", "SSTABLEPATH=>TABLEVIEWNAME", "SSTABLEPATH=>KEYSPACETABLEVIEWNAME", "SSTABLEPATHS=>TABLEVIEWNAME", "SSTABLEPATHS=>KEYSPACETABLEVIEWNAME" };

        readonly CacheInfo[] CachedInfo = new CacheInfo[] { new CacheInfo(), new CacheInfo(), new CacheInfo() };
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

            return null;
        }

        #endregion

        [Flags]
        public enum SessionLookupActions
        {
            Default = 0,
            Label = 0x0001,
            Session = 0x0002,
            Read = 0x0004,
            Define = 0x0008,
            Delete = 0x0010,
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

        public CLogLineTypeParser()
        {
            if (SessionLookupAction == SessionLookupActions.Default)
                this.SessionLookupAction = SessionLookupActions.ReadSession;
        }

        public CLogLineTypeParser(string levelMatchRegEx,
                                    string threadidMatchRegEx,
                                    string filenameMatchRegEx,
                                    string filelineMatchRegEx,
                                    string messageMatchRegEx,
                                    string parsemessageRegEx,
                                    string parsethreadidRegEx,
                                    string sessionKey,
                                    EventTypes eventType,
                                    EventClasses eventClass,
                                    string subClass = null,
                                    Version matchVersion = null,
                                    DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                    string[] examples = null)
            : this(string.IsNullOrEmpty(levelMatchRegEx) ? null : new RegExParseString(levelMatchRegEx),
                      string.IsNullOrEmpty(threadidMatchRegEx) ? null : new RegExParseString(threadidMatchRegEx),
                      string.IsNullOrEmpty(filenameMatchRegEx) ? null : new RegExParseString(filenameMatchRegEx),
                      string.IsNullOrEmpty(filelineMatchRegEx) ? null : new RegExParseString(filelineMatchRegEx),
                      string.IsNullOrEmpty(messageMatchRegEx) ? null : new RegExParseString(messageMatchRegEx),
                      string.IsNullOrEmpty(parsemessageRegEx) ? null : new RegExParseString(parsemessageRegEx),
                      string.IsNullOrEmpty(threadidMatchRegEx) ? null : new RegExParseString(threadidMatchRegEx),
                      sessionKey,
                      eventType,
                      eventClass,
                      subClass,
                      matchVersion,
                      product,
                      examples)
        { }

        public CLogLineTypeParser(RegExParseString levelMatchRegEx,
                                    RegExParseString threadidMatchRegEx,
                                    RegExParseString filenameMatchRegEx,
                                    RegExParseString filelineMatchRegEx,
                                    RegExParseString messageMatchRegEx,
                                    RegExParseString parsemessageRegEx,
                                    RegExParseString parsethreadidRegEx,
                                    string sessionKey,
                                    EventTypes eventType,
                                    EventClasses eventClass,
                                    string subClass = null,
                                    Version matchVersion = null,
                                    DSEInfo.InstanceTypes product = DSEInfo.InstanceTypes.Cassandra,
                                    string[] examples = null)
        {
            this.MatchVersion = matchVersion;
            this.LevelMatch = levelMatchRegEx;
            this.ThreadIdMatch = threadidMatchRegEx;
            this.FileNameMatch = filenameMatchRegEx;
            this.FileLineMatch = filelineMatchRegEx;
            this.MessageMatch = messageMatchRegEx;
            this.ParseMessage = parsemessageRegEx;
            this.ParseThreadId = parsethreadidRegEx;
            this.EventType = eventType;
            this.EventClass = eventClass;
            this.SubClass = subClass;
            this.Product = product;
            this.SessionKey = sessionKey;
            this.Examples = examples;

            if (this.SessionLookupAction == SessionLookupActions.Default)
                this.SessionLookupAction = SessionLookupActions.ReadSession;
        }

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
        /// If true, the event is ignored and not processed.
        /// </summary>
        public bool IgnoreEvent { get; set; }

        private const int SubClassIdx = 1;
        private string _subclass = null;
        /// <summary>
        /// e.g., Hint, Tombstone, etc. or null
        /// This takes the same values as <see cref="SessionKey"/>
        /// Callers should call the <see cref="DetermineSubClass(Cluster, INode, IKeyspace, IDictionary{string, object}, ILogMessage, IList{LogCassandraEvent}, IList{Tuple{string, List{LogCassandraEvent}}})"/> method to obtain the correct value.
        /// </summary>
        public string SubClass
        {
            get { return this._subclass; }
            set
            {
                if(value != this._subclass)
                {
                    this._subclass = value;
                    CacheSessionKey(this, ref this._subclass, SubClassIdx);
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

        public string[] Examples;

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
                                        logLineMessage);
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

        #region Private Methods
        private static void CacheSessionKey(CLogLineTypeParser logLineParser,
                                            ref string propValue,
                                            int cacheIdx)
        {
            var cacheInfo = logLineParser.GetCacheInfo(cacheIdx);

            if (logLineParser.EventType == EventTypes.SingleInstance)
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
                cacheInfo.KeyMethodInfo = MiscHelpers.CompileMethod(string.Format("{0}_{1}_{2}_Get{3}",
                                                                                    logLineParser.Product.ToString(),
                                                                                    logLineParser.EventClass.ToString(),
                                                                                    logLineParser.EventType.ToString(),
                                                                                    logLineParser.CacheInfoVarName(cacheIdx)),
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

            if (propValue[0] == '"' || propValue[0] == '\'')
            {
                cacheInfo.KeyIsStatic = true;
                cacheInfo.KeyValue = StringHelpers.RemoveQuotes(propValue);
            }
            else if (propValue[0] == '{')
            {
                cacheInfo.KeyMethodInfoIsMethod = false;
                cacheInfo.KeyMethodInfo = MiscHelpers.CompileMethod(string.Format("{0}_{1}_{2}_Determine{3}",
                                                                                        logLineParser.Product.ToString(),
                                                                                        logLineParser.EventClass.ToString(),
                                                                                        logLineParser.EventType.ToString(),
                                                                                        logLineParser.CacheInfoVarName(cacheIdx)),
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
                if(!logLineProperties.TryGetValue(varName, out objSessionKey))
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
                    return ((IEnumerable<object>)objSessionKey).Sum(s => s.GetHashCode()).ToString();
                }
                else
                {
                    return objSessionKey.ToString();
                }
            }

            return string.Empty;
        }

        private static string DetermineKeyValue(CLogLineTypeParser logLineParser,
                                                int cacheIdx,
                                                Cluster cluster,
                                                INode node,
                                                IKeyspace keyspace,
                                                IDictionary<string, object> logLineProperties,
                                                ILogMessage logLineMessage)
        {
            if (logLineParser.EventType == EventTypes.SingleInstance)
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
                string sessionKey;

                if (!GenerateSessionKey(logLineParser,
                                        cacheIdx,
                                        cluster,
                                        node,
                                        keyspace,
                                        logLineProperties,
                                        logLineMessage,
                                        out sessionKey))
                {
                    if (sessionKey == null)
                    {
                        throw new ArgumentException(string.Format("{0} \"{1}\" was not valid.",
                                                                    logLineParser.CacheInfoVarName(cacheIdx),
                                                                    logLineParser.CacheInfoVarValue(logLineParser, cacheIdx)),
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
            object objSessionKey = null;
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

        public CLogLineTypeParser[] Parsers { get; set; }

        public CLogLineTypeParser[] DetermineParsers(Version dseVersion)
        {
            var parserList = this.Parsers.Where(m => m.MatchVersion == null).ToList();

            if (dseVersion != null)
            {
                var versionedMapper = this.Parsers.Where(m => m.MatchVersion != null && dseVersion >= m.MatchVersion)
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
        /// <returns>
        /// Returns null to indicate that the log event did not match anything.
        /// Returns a tuple where the
        ///     first item is the regex for the thread id or null,
        ///     second item is the match for the thread id or null,
        ///     third item is the regex for the message or null,
        ///     fourth item is the match for the message or null,
        ///     fifth item is the parser instance.
        ///     first and third will never both be be null.
        /// </returns>
        public static Tuple<Regex, Match, Regex, Match, CLogLineTypeParser> FindMatch (CLogLineTypeParser[] parsers, ILogMessage logEvent)
        {
            foreach (var parserItem in parsers)
            {
                if ((parserItem.FileLineMatch?.IsMatchAny(logEvent.FileLine.ToString()) ?? true)
                        && (parserItem.FileNameMatch?.IsMatchAny(logEvent.FileName) ?? true)
                        && (parserItem.LevelMatch?.IsMatchAny(logEvent.Level.ToString()) ?? true)
                        && (parserItem.ThreadIdMatch?.IsMatchAny(logEvent.ThreadId) ?? true)
                        && (parserItem.MessageMatch?.IsMatchAny(logEvent.Message) ?? true))
                {
                    Regex threadIdRegEx = null;
                    Regex messageRegEx = null;
                    var threadIdMatch = parserItem.ParseThreadId?.MatchFirstSuccessful(logEvent.ThreadId, out threadIdRegEx);
                    var messageMatch = parserItem.ParseMessage?.MatchFirstSuccessful(logEvent.Message, out messageRegEx);

                    if(threadIdMatch != null || messageMatch != null)
                    {
                        return new Tuple<Regex, Match, Regex, Match, CLogLineTypeParser>(threadIdRegEx,
                                                                                            threadIdMatch,
                                                                                            messageRegEx,
                                                                                            messageMatch,
                                                                                            parserItem);
                    }
                }
            }

            return null;
        }
    }
}

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
        public CLogLineTypeParser() { }

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
            this.Examples = null;
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
        ///
        /// There are special group names. They are:
        ///     &quot;SUBCLASS&quot                                 -- this will map the capture group&apos;s value to property &quot;SubClass&quot; of LogCassandraEvent class
        ///     &quot;ID&quot;                                      -- this will map the capture group&apos;s value to property &quot;Id&quot; of LogCassandraEvent class. It must be a valid GUID.
        ///     &quot;DURATION_xxx&quot; or &quot;DURATION&quot;    -- this will map the capture group&apos;s value to property &quot;Duration&quot; of LogCassandraEvent class
        ///                                                             &quot;xxx&quot; is the time unit. If not provided the time unit must be part of the capture (e.g., &quot;10 ms&quot;)
        ///     &quot;EVENTDURATIONDATETIME&quot;                   -- this will map the capture group&apos;s value to property &quot;EventTimeDuration&quot; and calaculates the &quot;Duration&quot; of the LogCassandraEvent class
        ///                                                             The value must be valid date-time syntax
        ///     &quot;KEYSPACE&quot;                                -- this will map the capture group&apos;s value to property &quot;Keyspace&quot; of the LogCassandraEvent class
        ///     &quot;DDLITEMNAME&quot;                             -- this will map the capture group&apos;s value to property &quot;DDLItems&quot; of the LogCassandraEvent class.
        ///                                                             This can be table, view, or index qualifier with optional keyspace qualifier. The separator can be a period or slash.
        ///     &quot;SSTABLEPATH&quot;                             -- this will be used to obtain the keyspace and DDL item plus map to the capture group&apos;s value to property &quot;SSTables&quot; of the LogCassandraEvent class
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
        /// e.g., Hint, Tombstone, etc. or null
        /// </summary>
        public string SubClass { get; set; }
        public DSEInfo.InstanceTypes Product { get; set; }

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
        ///         A capture group name (e.g., SSTABLEPATH) as defined in the <see cref="ParseMessage"/> and <see cref="ParseThreadId"/> properties
        ///         SSTABLEPATH=>DDLITEMNAME -- obtains the DDL Item name from the SSTable Path string
        ///         SSTABLEPATH=>KEYSPACE -- obtains the keyspace name from the SSTable Path string
        /// --or--
        ///     A C# expression that returns the following based on EventType property:
        ///         -- EventTypes.TimespanBegin, must return a string that represents the session key. This string should be able to be generated for any Timespan sessions and will be used to find the LogCassandraEvent instance when the log line is not a TimespanBegin.
        ///             This session key is item1 in the Tuple in the openTimeSpanSessions local variable.
        ///         -- EventTypes.Timespan and EventTypes.TimespanEnd, must return LogCassandraEvent instance found in the openTimeSpanSessions or timespanSessions local variables based on the session key generated on the TimespanBegin event.
        ///     This express must start with a &apos;{&apos; and end with a &apos;}&apos;
        ///     It must follow C# syntax. It has the following locally defined variables:
        ///         <code>clogLineTypeParser</code> -- A CLogLineTypeParser instance which is an instance of this information
        ///         <code>cluster</code>            -- A Cluster instance assocated with this log parsing file
        ///         <code>node</code>               -- An INode instance assocated with this log parsing file
        ///         <code>keyspace</code>           -- Current IKeyspace instance assocated with this log line. Could be null.
        ///         <code>logLineProperties</code>  -- A IDictionary&lt;string, object&gt; instance where the key is the capture group&apos;s name and the assocated value
        ///         <code>logLineMessage</code>     -- An ILogMessage instance that consistence of this parsed log line
        ///         <code>timespanSessions</code>   -- A IList<LogCassandraEvent> of all timespan sessions for this log file
        ///         <code>openTimeSpanSessions</code> -- A IList<Tuple<string,LogCassandraEvent>> of only open timespan sessions for this log file. The first item is the SesionKey and the second item is always the TimespanBegin LogCassandraEvent instance.
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
                    this.CacheSessionKey();
                }
            }
        }

        public string[] Examples;
        private System.Reflection.MethodInfo _sessionkeyMethodInfo = null;
        private bool _sessionkeyKeywordIsGroupName = false;
        private string _sessionkeyKeyword = null;
        private readonly static string[] SessionKeywords = new string[] { "ThreadId", "FileName", "FileNameLine", "SSTABLEPATH=>DDLITEMNAME", "SSTABLEPATH=>KEYSPACE" };

        public LogCassandraEvent DetermineOpenSession(Cluster cluster,
                                                        INode node,
                                                        IKeyspace keyspace,
                                                        IDictionary<string, object> logLineProperties,
                                                        ILogMessage logLineMessage,
                                                        IList<LogCassandraEvent> timespanSessions,
                                                        IList<Tuple<string, LogCassandraEvent>> openTimeSpanSessions)
        {
            if (this.EventType == EventTypes.SingleInstance || this.EventType == EventTypes.SessionBegin)
            {
                return null;
            }

            if (this._sessionkeyMethodInfo != null)
            {
                return this._sessionkeyMethodInfo.Invoke(null, new object[] { cluster, node, keyspace, logLineProperties, logLineMessage, timespanSessions, openTimeSpanSessions }) as LogCassandraEvent;
            }
            else
            {
                string sessionKey;

                if (!this.GetSessionKey(logLineProperties, logLineMessage, out sessionKey))
                {
                    if (sessionKey == null)
                    {
                        throw new ArgumentException(string.Format("SessionKey \"{0}\" was not valid.", this._sessionKey), "SessionKey");
                    }
                }

                return openTimeSpanSessions.LastOrDefault(s => s.Item1 == sessionKey)?.Item2;
            }
        }

        public string DetermineSessionKey(Cluster cluster,
                                            INode node,
                                            IKeyspace keyspace,
                                            IDictionary<string, object> logLineProperties,
                                            ILogMessage logLineMessage,
                                            IList<LogCassandraEvent> timespanSessions,
                                            IList<Tuple<string, LogCassandraEvent>> openTimeSpanSessions)
        {
            if (this.EventType == EventTypes.SingleInstance || this.EventType == EventTypes.SessionItem || this.EventType == EventTypes.SessionEnd)
            {
                return null;
            }

            if (this._sessionkeyMethodInfo != null)
            {
                return this._sessionkeyMethodInfo.Invoke(null, new object[] { cluster, node, keyspace, logLineProperties, logLineMessage, timespanSessions, openTimeSpanSessions }) as string;
            }
            else
            {
                string sessionKey;

                if (!this.GetSessionKey(logLineProperties, logLineMessage, out sessionKey))
                {
                    if (sessionKey == null)
                    {
                        throw new ArgumentException(string.Format("SessionKey \"{0}\" was not valid.", this._sessionKey), "SessionKey");
                    }
                }

                return sessionKey;
            }
        }

        private void CacheSessionKey()
        {
            if (this.EventType == EventTypes.SingleInstance)
            {
                this._sessionkeyMethodInfo = null;
                this._sessionkeyKeyword = null;
                this._sessionkeyKeywordIsGroupName = false;
                return;
            }

            this._sessionKey = this._sessionKey?.Trim();

            if (string.IsNullOrEmpty(this._sessionKey))
            {
                this._sessionkeyMethodInfo = null;
                this._sessionkeyKeyword = "ID";
                this._sessionkeyKeywordIsGroupName = true;
                return;
            }

            if (this._sessionKey[0] == '{')
            {
                bool beginTimespan = this.EventType == EventTypes.SessionBegin;
                this._sessionkeyMethodInfo = MiscHelpers.CompileMethod(string.Format("{0}_{1}_{2}_{3}",
                                                                                        this.Product,
                                                                                        this.EventClass,
                                                                                        this.EventType,
                                                                                        beginTimespan ? "DetermineSessionKey" : "DetermineOpenSession"),
                                                                        beginTimespan ? typeof(string) : typeof(LogCassandraEvent),
                                                                        new Tuple<Type, string>[]
                                                                        {
                                                                            new Tuple<Type, string>(typeof(CLogLineTypeParser), "clogLineTypeParser"),
                                                                            new Tuple<Type,string>(typeof(Cluster), "cluster"),
                                                                            new Tuple<Type,string>(typeof(INode), "node"),
                                                                            new Tuple<Type,string>(typeof(IKeyspace), "keyspace"),
                                                                            new Tuple<Type,string>(typeof(IDictionary<string,object>), "logLineProperties"),
                                                                            new Tuple<Type,string>(typeof(ILogMessage), "logLineMessage"),
                                                                            new Tuple<Type,string>(typeof(IList<LogCassandraEvent>), "timespanSessions"),
                                                                            new Tuple<Type,string>(typeof(IList<Tuple<string, LogCassandraEvent>>), "openTimeSpanSessions")
                                                                        },
                                                                        this._sessionKey);
            }
            else
            {
                this._sessionkeyKeyword = this._sessionKey;
                this._sessionkeyKeywordIsGroupName = !SessionKeywords.Contains(this._sessionKey);
            }
        }

        private bool GetSessionKey(IDictionary<string, object> logLineProperties,
                                            ILogMessage logLineMessage,
                                            out string sessionKey)
        {
            if (this._sessionkeyKeyword != null)
            {
                object objSessionKey = null;

                if (this._sessionkeyKeywordIsGroupName)
                {
                    logLineProperties.TryGetValue(this._sessionkeyKeyword, out objSessionKey);
                }
                else
                {
                    switch (this._sessionkeyKeyword)
                    {
                        case "ThreadId":
                            objSessionKey = logLineMessage.ThreadId;
                            break;
                        case "FileName":
                            objSessionKey = logLineMessage.FileName;
                            break;
                        case "FileNameLine":
                            objSessionKey = logLineMessage.FileName + ":" + logLineMessage.FileLine.ToString();
                            break;
                        case "SSTABLEPATH=>DDLITEMNAME":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, false);
                            break;
                        case "SSTABLEPATH=>KEYSPACE":
                            objSessionKey = DetermineNameFromSSTablePath(logLineProperties, true);
                            break;
                        default:
                            break;
                    }
                }

                if (objSessionKey is string)
                {
                    sessionKey = (string)objSessionKey;
                    return true;
                }
                else if (objSessionKey is System.Collections.IEnumerable)
                {
                    sessionKey = ((IEnumerable<object>)objSessionKey).Sum(s => s.GetHashCode()).ToString();
                    return false;
                }
            }

            sessionKey = null;
            return false;
        }

        private string DetermineNameFromSSTablePath(IDictionary<string, object> logLineProperties, bool returnKeyspaceName)
        {
            object objSessionKey = null;
            string name = null;

            logLineProperties.TryGetValue("SSTABLEPATH", out objSessionKey);

            if (objSessionKey != null)
            {
                string path = null;

                if (objSessionKey is string)
                {
                    path = (string)objSessionKey;
                }
                else if (objSessionKey is System.Collections.IEnumerable)
                {
                    path = ((IEnumerable<string>)objSessionKey).FirstOrDefault();
                }

                var ksddlnameItems = StringHelpers.ParseSSTableFileIntoKSTableNames(path);

                if(ksddlnameItems != null)
                {
                    if (returnKeyspaceName)
                        name = ksddlnameItems.Item1;
                    else
                        name = ksddlnameItems.Item2;
                }
            }
            return name;
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

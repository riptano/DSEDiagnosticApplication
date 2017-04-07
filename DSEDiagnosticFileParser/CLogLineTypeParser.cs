using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    public sealed class CLogLineTypeParser
    {
        public CLogLineTypeParser(string levelMatchRegEx,
                                    string threadidMatchRegEx,
                                    string filenameMatchRegEx,
                                    string filelineMatchRegEx,
                                    string messageMatchRegEx,
                                    string parsemessageRegEx,
                                    EventTypes eventType,
                                    EventClasses eventClass,
                                    string subClass)
            : this(string.IsNullOrEmpty(levelMatchRegEx) ? null : new RegExParseString(levelMatchRegEx),
                      string.IsNullOrEmpty(threadidMatchRegEx) ? null : new RegExParseString(threadidMatchRegEx),
                      string.IsNullOrEmpty(filenameMatchRegEx) ? null : new RegExParseString(filenameMatchRegEx),
                      string.IsNullOrEmpty(filelineMatchRegEx) ? null : new RegExParseString(filelineMatchRegEx),
                      string.IsNullOrEmpty(messageMatchRegEx) ? null : new RegExParseString(messageMatchRegEx),
                      string.IsNullOrEmpty(parsemessageRegEx) ? null : new RegExParseString(parsemessageRegEx),
                      eventType,
                      eventClass,
                      subClass)
        { }

        public CLogLineTypeParser(RegExParseString levelMatchRegEx,
                                    RegExParseString threadidMatchRegEx,
                                    RegExParseString filenameMatchRegEx,
                                    RegExParseString filelineMatchRegEx,
                                    RegExParseString messageMatchRegEx,
                                    RegExParseString parsemessageRegEx,
                                    EventTypes eventType,
                                    EventClasses eventClass,
                                    string subClass)
        {
            this.LevelMatch = levelMatchRegEx;
            this.ThreadIdMatch = threadidMatchRegEx;
            this.FileNameMatch = filenameMatchRegEx;
            this.FileLineMatch = filelineMatchRegEx;
            this.MessageMatch = messageMatchRegEx;
            this.ParseMessage = parsemessageRegEx;
            this.EventType = eventType;
            this.EventClass = eventClass;
            this.SubClass = subClass;
        }

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
        /// The named capture group can explictly define the group&apos;s value type by placing type name after the last underscore.
        /// e.g., &quot;(?&lt;mygroupname_int&gt;.+)&quot; where the property key is &quot;mygroupname&quot; and the group&apos;s value type is an int.
        ///
        /// There is a special group name of &quot;LogSubClass&quot; that will map this capture group&apos;s value to property SubClass.
        /// </summary>
        public RegExParseString ParseMessage { get; set; }
        public EventTypes EventType { get; set; }
        public EventClasses EventClass { get; set; }
        /// <summary>
        /// e.g., Hint, Tombstone, etc. or null
        /// </summary>
        public string SubClass { get; set; }
    }

    public sealed class CLogTypeParser
    {
        public CLogTypeParser(string logClass, Version matchCersion, params CLogLineTypeParser[] args)
        {
            this.LogClass = logClass;
            this.MatchVersion = matchCersion;
            this.Parrsers = args;
        }

        /// <summary>
        /// The name of the class used to parse the Cassandra log file. e.g., Log_CLog4Net
        /// </summary>
        public string LogClass { get; set; }
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

        public CLogLineTypeParser[] Parrsers { get; set; }
    }
}

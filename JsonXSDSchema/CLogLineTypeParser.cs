using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using System.Threading;


namespace JsonXSDSchema
{
    public sealed class RegExParseString
    {
       
        public RegExParseString() { }
        
        public readonly string[] RegExStrings;       
    }

    public sealed class CLogTypeParser
    {

        public CLogTypeParser() { }

        public string LogClass { get; set; }

        public CLogLineTypeParser[] Parsers { get; set; }
    }
    public sealed class CLogLineTypeParser
    {
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
            TagLogProp = 0x0800
        }
        
        public CLogLineTypeParser()
        {
        }

        public decimal TagId;
        public string Description;

        public decimal? SessionBeginTagId = null;


        public Version MatchVersion { get; set; }

        public RegExParseString LevelMatch { get; set; }

        public RegExParseString ThreadIdMatch { get; set; }

        public RegExParseString FileNameMatch { get; set; }

        public RegExParseString FileLineMatch { get; set; }

        public RegExParseString MessageMatch { get; set; }

        public RegExParseString ParseMessage { get; set; }

        public RegExParseString ParseThreadId { get; set; }
        public EventTypes EventType { get; set; }
        public EventClasses EventClass { get; set; }


        public long MaxNumberOfEvents { get; set; } = -1;

        public long MaxNumberOfEventsPerNode { get; set; } = -1;

        public long RunningCount = 0;


        public bool IgnoreEvent { get; set; }


        public PropertyInherentOptions PropertyInherentOption { get; set; } = PropertyInherentOptions.None;


        public bool LogPropertySessionMerge { get; set; }


        public bool AssociateEventToNode { get; set; } = true;

        public string SubClass
        {
            get;
            set;
        }


        public string AnalyticsGroup { get; set; }


        public string[] DeltaRunningTotalProperty { get; set; }


        public string DeltaRunningTotalKey
        {
            get;
            set;
        }

        public DSEInfo.InstanceTypes Product { get; set; }

        public string SessionKey
        {
            get;
            set;
        }

        public SessionKeyActions SessionKeyAction { get; set; }

        public string SessionLookup
        {
            get;
            set;
        }

        public SessionLookupActions SessionLookupAction { get; set; }

        public SessionParentActions SessionParentAction { get; set; } = SessionParentActions.Default;
        public string[] Examples;

        public CLogLineTypeParser SessionBeginReference { get; internal set; } = null;

        public bool IsClone { get; private set; } = false;


        public decimal? LinkedTagId
        {
            get;
            set;
        }
    }
}

﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace DSEDiagnosticLibrary.Properties {
    
    
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.VisualStudio.Editors.SettingsDesigner.SettingsSingleFileGenerator", "14.0.0.0")]
    internal sealed partial class Settings : global::System.Configuration.ApplicationSettingsBase {
        
        private static Settings defaultInstance = ((Settings)(global::System.Configuration.ApplicationSettingsBase.Synchronized(new Settings())));
        
        public static Settings Default {
            get {
                return defaultInstance;
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<ArrayOfString xmlns:xsi=\"http://www.w3." +
            "org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n  <s" +
            "tring>+</string>\r\n</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection HostNamePathNameCharSeparators {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["HostNamePathNameCharSeparators"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("MiB")]
        public string DefaultStorageSizeUnit {
            get {
                return ((string)(this["DefaultStorageSizeUnit"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("SEC")]
        public string DefaultTimeUnit {
            get {
                return ((string)(this["DefaultTimeUnit"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"(?:(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))(%[0-9A-Za-z]+)?(/[1-9]{1,3})?)")]
        public string IPAdressRegEx {
            get {
                return ((string)(this["IPAdressRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"[
{""ConfigType"":""DSE"",""ContainsString"":""dse.yaml"",""MatchAction"":""FileNamewExtension,Equals""},
{""ConfigType"":""DSE"",""ContainsString"":""dse-env.sh"",""MatchAction"":""FileNamewExtension,Equals""},
{""ConfigType"":""Solr"",""ContainsString"":""solr"",""MatchAction"":""FileNameOnly,StartsWith""},
{""ConfigType"":""Spark"",""ContainsString"":""spark"",""MatchAction"":""FileNameOnly,StartsWith""},
{""ConfigType"":""Spark"",""ContainsString"":""spark"",""MatchAction"":""FileNameOnly,EndsWith""},
{""ConfigType"":""Spark"",""ContainsString"":""dse-spark"",""MatchAction"":""FileNameOnly,StartsWith""},
{""ConfigType"":""Hadoop"",""ContainsString"":""hadoop"",""MatchAction"":""FileNameOnly,Contains""},
{""ConfigType"":""Snitch"",""ContainsString"":""cassandra-topology.properties"",""MatchAction"":""FileNamewExtension,Equals""},
{""ConfigType"":""Snitch"",""ContainsString"":""cassandra-rackdc.properties"",""MatchAction"":""FileNamewExtension,Equals""},
{""ConfigType"":""Snitch"",""ContainsString"":""cassandra-topology.yaml"",""MatchAction"":""FileNamewExtension,Equals""},
{""ConfigType"":""Cassandra"",""ContainsString"":""cassandra.yaml"",""MatchAction"":""FileNamewExtension,Equals""},
{""ConfigType"":""Cassandra"",""ContainsString"":""cassandra-env.sh"",""MatchAction"":""FileNamewExtension,Equals""}
]")]
        public string ConfigTypeMappers {
            get {
                return ((string)(this["ConfigTypeMappers"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("MiB")]
        public string DefaultMemorySizeUnit {
            get {
                return ((string)(this["DefaultMemorySizeUnit"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("MiB, SEC")]
        public string DefaultStorageRate {
            get {
                return ((string)(this["DefaultStorageRate"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("MiB, SEC")]
        public string DefaultMemoryRate {
            get {
                return ((string)(this["DefaultMemoryRate"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("9")]
        public int UnitOfMeasureRoundDecimals {
            get {
                return ((int)(this["UnitOfMeasureRoundDecimals"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<ArrayOfString xmlns:xsi=\"http://www.w3." +
            "org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n  <s" +
            "tring>map</string>\r\n  <string>set</string>\r\n  <string>list</string>\r\n</ArrayOfSt" +
            "ring>")]
        public global::System.Collections.Specialized.StringCollection CQLCollectionTypes {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["CQLCollectionTypes"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("(?:\\s|^)tuple\\s*\\<")]
        public string TupleRegEx {
            get {
                return ((string)(this["TupleRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("(?:\\s|^)frozen\\s*\\<")]
        public string FrozenRegEx {
            get {
                return ((string)(this["FrozenRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("\\sstatic(?:\\s|$)")]
        public string StaticRegEx {
            get {
                return ((string)(this["StaticRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("\\sprimary\\s+key(?:\\s|$)")]
        public string PrimaryKeyRegEx {
            get {
                return ((string)(this["PrimaryKeyRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("(?:\\s|^)blob(?:\\s|$)")]
        public string BlobRegEx {
            get {
                return ((string)(this["BlobRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("(?:\\s|^)counter(?:\\s|$)")]
        public string CounterRegEx {
            get {
                return ((string)(this["CounterRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"<?xml version=""1.0"" encoding=""utf-16""?>
<ArrayOfString xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
  <string>system_auth</string>
  <string>system_distributed</string>
  <string>system_schema</string>
  <string>system</string>
  <string>system_traces</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection SystemKeyspaces {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["SystemKeyspaces"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"<?xml version=""1.0"" encoding=""utf-16""?>
<ArrayOfString xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
  <string>dse_system</string>
  <string>dse_security</string>
  <string>solr_admin</string>
  <string>dse_auth</string>
  <string>dse_leases</string>
  <string>dse_perf</string>
  <string>OpsCenter</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection DSEKeyspaces {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["DSEKeyspaces"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<ArrayOfString xmlns:xsi=\"http://www.w3." +
            "org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n  <s" +
            "tring>system_traces</string>\r\n  <string>dse_perf</string>\r\n</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection PerformanceKeyspaces {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["PerformanceKeyspaces"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"<?xml version=""1.0"" encoding=""utf-16""?>
<ArrayOfString xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
  <string>system.paxos</string>
  <string>system_traces</string>
  <string>dse_perf</string>
  <string>system_auth</string>
  <string>dse_security</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection TablesUsageFlag {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["TablesUsageFlag"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<ArrayOfString xmlns:xsi=\"http://www.w3." +
            "org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n  <s" +
            "tring>org.apache.cassandra.index.sasi.SASIIndex</string>\r\n</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection IsSasIIIndexClasses {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["IsSasIIIndexClasses"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"<?xml version=""1.0"" encoding=""utf-16""?>
<ArrayOfString xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
  <string>com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex</string>
  <string>com.datastax.bdp.search.solr.ThriftSolrSecondaryIndex</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection IsSolrIndexClass {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["IsSolrIndexClass"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("^\\s*\\(?(\\-?\\d+)\\s*\\,\\s*(\\-?\\d+)\\s*\\]?\\s*$")]
        public string TokenRangeRegEx {
            get {
                return ((string)(this["TokenRangeRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"<?xml version=""1.0"" encoding=""utf-16""?>
<ArrayOfString xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
  <string>tmp</string>
  <string>ka</string>
  <string>jb</string>
  <string>ja</string>
  <string>ic</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection SSTableVersionMarkers {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["SSTableVersionMarkers"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("^(?:([a-z0-9\\-_$%+=@!?<>^*&]+)\\-([0-9a-f]{32})$|([a-z0-9\\-_$%+=@!?<>^*&]+)$)")]
        public string SSTableColumnFamilyRegEx {
            get {
                return ((string)(this["SSTableColumnFamilyRegEx"]));
            }
        }
    }
}

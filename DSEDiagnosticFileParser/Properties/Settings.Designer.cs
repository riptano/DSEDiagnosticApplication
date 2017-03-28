﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace DSEDiagnosticFileParser.Properties {
    
    
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
        [global::System.Configuration.DefaultSettingValueAttribute("[{\"item1\":\".zip\",\"item2\":\"zip\"},{\"item1\":\".tar\",\"item2\":\"tar\"},{\"item1\":\".gz\",\"it" +
            "em2\":\"gz\"}] ")]
        public string ExtractFilesWithExtensions {
            get {
                return ((string)(this["ExtractFilesWithExtensions"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("[\r\n{\"Catagory\": \"ZipFile\", \"FilePatterns\": [\".\\\\*.tar\",\".\\\\*.tar.gz\",\".\\\\*.zip\"]," +
            " \"FileParsingClass\": \"DSEDiagnosticFileParser.file_unzip\", \"NodeIdPos\": 0, \"Proc" +
            "essingTaskOption\":\"IgnoreNode\", \"ProcessPriorityLevel\":1000},\r\n{\"Catagory\": \"Com" +
            "mandOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\status\"], \"FileParsing" +
            "Class\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"Process" +
            "ingTaskOption\":\"IgnoreNode,OnlyOnce\", \"ProcessPriorityLevel\":950},\r\n{\"Catagory\":" +
            " \"CommandOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\dsetool\\\\ring\"], \"FileParsi" +
            "ngClass\": \"DSEDiagnosticFileParser.file_dsetool_ring\", \"NodeIdPos\": 0, \"Processi" +
            "ngTaskOption\":\"IgnoreNode,OnlyOnce\", \"ProcessPriorityLevel\":950},\r\n{\"Catagory\": " +
            "\"CommandOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\ring\"], \"FileParsi" +
            "ngClass\": \"DSEDiagnosticFileParser.file_nodetool_ring\", \"NodeIdPos\": 0, \"Process" +
            "ingTaskOption\":\"IgnoreNode,OnlyOnce\", \"ProcessPriorityLevel\":950},\r\n{\"Catagory\":" +
            " \"SystemOutputFile\", \"FilePatterns\": [\".\\\\opscenterd\\\\node_info.json\"], \"FilePar" +
            "singClass\": \"DSEDiagnosticFileParser.json_node_info\", \"NodeIdPos\": -1, \"Processi" +
            "ngTaskOption\":\"AllNodesInDataCenter\",\"ProcessPriorityLevel\":900},\r\n{\"Catagory\": " +
            "\"SystemOutputFile\", \"FilePatterns\": [\".\\\\opscenterd\\\\repair_service.json\"], \"Fil" +
            "eParsingClass\": \"DSEDiagnosticFileParser.json_repair_service\", \"NodeIdPos\": -1, " +
            "\"ProcessingTaskOption\":\"AllNodesInDataCenter\",\"ProcessPriorityLevel\":900},\r\n{\"Ca" +
            "tagory\": \"ConfigurationFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\dse\"], \"FileParsing" +
            "Class\": \"DSEDiagnosticFileParser.file_DSE_env\", \"NodeIdPos\": -1, \"ProcessingTask" +
            "Option\":\"ScanForNode,ParallelProcessingWithinPriorityLevel\",\"ProcessPriorityLeve" +
            "l\":875},\r\n{\"Catagory\": \"ConfigurationFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\*.yam" +
            "l\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_yaml\", \"NodeIdPos\": -1, \"" +
            "ProcessingTaskOption\":\"ScanForNode,ParallelProcessingWithinPriorityLevel,Paralle" +
            "lProcessFiles\",\"IgnoreFilesMatchingRegEx\":\"\\\\\\\\cassandra-topology\\\\.yaml\", \"Proc" +
            "essPriorityLevel\":850},\r\n{\"Catagory\": \"ConfigurationFile\", \"FilePatterns\": [\".\\\\" +
            "nodes\\\\*\\\\cassandra-topology.*\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.f" +
            "ile_cassandra_topology\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,Pa" +
            "rallelProcessingWithinPriorityLevel,ParallelProcessFiles\",\"ProcessPriorityLevel\"" +
            ":800},\r\n{\"Catagory\": \"ConfigurationFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\cassand" +
            "ra-rackdc.properties\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_cassan" +
            "dra_rackdc_properties\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,Par" +
            "allelProcessingWithinPriorityLevel,ParallelProcessFiles\",\"ProcessPriorityLevel\":" +
            "800},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\machine-i" +
            "nfo.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_machine_info\", \"No" +
            "deIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing,ParallelPro" +
            "cessFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"File" +
            "Patterns\": [\".\\\\nodes\\\\*\\\\os-info.json\"], \"FileParsingClass\": \"DSEDiagnosticFile" +
            "Parser.json_os_info\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,Paral" +
            "lelProcessing,ParallelProcessFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": " +
            "\"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\os-metrics\\\\cpu.json\"], \"File" +
            "ParsingClass\": \"DSEDiagnosticFileParser.jason_cpu\", \"NodeIdPos\": -1, \"Processing" +
            "TaskOption\":\"ScanForNode,ParallelProcessing,ParallelProcessFiles\", \"ProcessPrior" +
            "ityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\" +
            "\\os-metrics\\\\load_avg.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_" +
            "load_avg\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessi" +
            "ng,ParallelProcessFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"CommandOut" +
            "putFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\agent_version.json\"], \"FileParsingClass" +
            "\": \"DSEDiagnosticFileParser.file_agent_version\", \"NodeIdPos\": -1, \"ProcessingTas" +
            "kOption\":\"ScanForNode,ParallelProcessing,ParallelProcessFiles\", \"ProcessPriority" +
            "Level\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\ja" +
            "va_system_properties.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_j" +
            "ava_system_properties\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,Par" +
            "allelProcessing,ParallelProcessFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\"" +
            ": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\ntp\\\\ntpstat\"], \"FileParsin" +
            "gClass\": \"DSEDiagnosticFileParser.file_ntpstat\", \"NodeIdPos\": -1, \"ProcessingTas" +
            "kOption\":\"ScanForNode,ParallelProcessing,ParallelProcessFiles\", \"ProcessPriority" +
            "Level\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nt" +
            "p\\\\ntptime\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_ntptime\", \"NodeI" +
            "dPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing,ParallelProces" +
            "sFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePat" +
            "terns\": [\".\\\\nodes\\\\*\\\\java_heap.json\"], \"FileParsingClass\": \"DSEDiagnosticFileP" +
            "arser.json_java_heap\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,Para" +
            "llelProcessing,ParallelProcessFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\":" +
            " \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\os-metrics\\\\memory.json\"], \"" +
            "FileParsingClass\": \"DSEDiagnosticFileParser.json_memory\", \"NodeIdPos\": -1, \"Proc" +
            "essingTaskOption\":\"ScanForNode,ParallelProcessing,ParallelProcessFiles\", \"Proces" +
            "sPriorityLevel\":100},\r\n{\"Catagory\": \"CommandOutputFile\", \"FilePatterns\": [\".\\\\no" +
            "des\\\\*\\\\nodetool\\\\info\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_node" +
            "tool_info\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcess" +
            "ing,ParallelProcessFiles\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"SystemOut" +
            "putFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\conf\\\\location.json\"], \"FileParsingClas" +
            "s\": \"DSEDiagnosticFileParser.json_location\", \"NodeIdPos\": -1, \"ProcessingTaskOpt" +
            "ion\":\"ScanForNode,ParallelProcessing,ParallelProcessFiles\", \"ProcessPriorityLeve" +
            "l\":100},\r\n{\"Catagory\": \"CQLFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\cqlsh\\\\describe" +
            "_schema\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.cql_ddl\", \"NodeIdPos\": -" +
            "1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPriorityLeve" +
            "l\":100},\r\n{\"Catagory\": \"LogFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\logs\\\\cassandra" +
            "\\\\system.log\"], \"FileParsingClass\": \"UserQuery.TestClass\", \"NodeIdPos\": -1}\r\n]")]
        public string ProcessFileMappings {
            get {
                return ((string)(this["ProcessFileMappings"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("{\r\n\"file_nodetool_status\":{\"RegExStrings\":[\"datacenter:\\\\s+([a-z0-9\\\\-_$%+=@!?<>^" +
            "*&]+)\\\\s*\",\r\n\t\t\t\t\t\t\"(un|ul|uj|um|dn|dl|dJ|dm)\\\\s+([a-z0-9.:_\\\\-+%]+)\\\\s+([0-9.]+" +
            ")\\\\s*([0-9a-z]{0,2})\\\\s+(\\\\d+)\\\\s+([0-9?.% ]+)\\\\s+([0-9a-f\\\\-]+)\\\\s+(.+)\"]},\r\n\"f" +
            "ile_ntpstat\":{\"RegExStrings\":[\"synchronised to NTP server\\\\s+\\\\((.+)\\\\).+stratum" +
            "\\\\s+(\\\\d+).+time correct.+within\\\\s+(\\\\d+\\\\s+[a-zA-Z]+).+polling.+every\\\\s+(\\\\d+" +
            "\\\\s+[a-zA-Z]+)\"]},\r\n\"file_ntptime\":{\"RegExStrings\":[\"ntp_adjtime.+frequency\\\\s+(" +
            "[0-9\\\\-.]+\\\\s+[a-zA-Z]+)\\\\,\\\\s+interval\\\\s+([0-9\\\\-.]+\\\\s+[a-zA-Z]+)\\\\,\\\\s+maxim" +
            "um error\\\\s+([0-9\\\\-.]+\\\\s+[a-zA-Z]+)\\\\,\\\\s+estimated error\\\\s+([0-9\\\\-.]+\\\\s+[a" +
            "-zA-Z]+)\\\\,.+time constant\\\\s+([0-9\\\\-.]+)\\\\,\\\\s+precision\\\\s+([0-9\\\\-.]+\\\\s+[a-" +
            "zA-Z]+)\\\\,\\\\s+tolerance\\\\s+([0-9]+\\\\s+[a-zA-Z]+)\",\r\n\t\t\t\t\t\"ntp_gettime.+maximum e" +
            "rror\\\\s+([0-9\\\\-.]+\\\\s+[a-zA-Z]+)\\\\,\\\\s+estimated error\\\\s+([0-9\\\\-.]+\\\\s+[a-zA-" +
            "Z]+)\"]},\r\n\"file_dsetool_ring\":{\"RegExStrings\":[\"([a-z0-9.:_\\\\-+%]+)\\\\s+([a-z0-9\\" +
            "\\-_$%+=@!?<>^*&]+)\\\\s+([a-z0-9\\\\-_$%+=@!?<>^*&]+)\\\\s+([a-z]+)\\\\s+([a-z]+)\\\\s+([a" +
            "-z]+)\\\\s+([a-z]+)\\\\s+([0-9.]+\\\\s*[a-z]{1,2})\\\\s+(\\\\w+|\\\\?)\\\\s+([0-9\\\\-]+)\"]},\r\n\"" +
            "file_nodetool_ring\":{\"RegExStrings\":[\"datacenter:\\\\s+([a-z0-9\\\\-_$%+=@!?<>^*&]+)" +
            "\\\\s*\",\"\\\\s*([0-9\\\\-]+)\",\r\n\t\t\t\t\t\"([a-z0-9.:_\\\\-+%]+)\\\\s+([a-z0-9\\\\-_$%+=@!?<>^*&]" +
            "+)\\\\s+([a-z]+)\\\\s+([a-z]+)\\\\s+([0-9.]+\\\\s*[a-z]{1,2})\\\\s+(\\\\w+|\\\\?)\\\\s+([0-9\\\\-]" +
            "+)\"]},\r\n\"cql_ddl\":{\"RegExStrings\":[\"use\\\\s+(?:\\\\\'|\\\\\\\")([a-z0-9\\\\-_$%+=@!?<>^*&]" +
            "+)(?:\\\\\'|\\\\\\\")\",\r\n\t\t\t\t\"create\\\\s+(?:keyspace|schema)\\\\s+(?:if\\\\s+not\\\\s+exists\\\\" +
            "s+)?(?:\\\\\'|\\\\\\\")?([a-z0-9\\\\-_$%+=@!?<>^*&]+)(?:\\\\\'|\\\\\\\")?\\\\s+with\\\\s+(replicatio" +
            "n)\\\\s*\\\\=\\\\s*(\\\\{.+\\\\})\\\\s*(?:and\\\\s+(durable_writes)\\\\s*\\\\=\\\\s*(\\\\w+))?\\\\s*\\\\;?" +
            "\",\r\n\t\t\t\t\"create\\\\s+(?:table|column\\\\s+family)\\\\s+(?:if\\\\s+not\\\\s+exists\\\\s+)?([a" +
            "-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+(?:\\\\(\\\\s*(.+)\\\\s*\\\\)\\\\s+with\\\\s+(.+)|(?:\\\\(" +
            "\\\\s*(.+)\\\\s*\\\\)))\\\\s*\\\\;?\",\r\n\t\t\t\t\"create\\\\s+type\\\\s+(?:if\\\\s+not\\\\s+exists\\\\s+)?" +
            "([a-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+(?:\\\\(\\\\s*(.+)\\\\s*\\\\))\\\\s*\\\\;?\",\r\n\t\t\t\t\"cr" +
            "eate\\\\s+(custom\\\\s+)?index\\\\s+(?:if\\\\s+not\\\\s+exists\\\\s+)?([a-z0-9\\\\-_$%+=@!?<>^" +
            "*&\\\\\'\\\\\\\"]+)\\\\s+on\\\\s+([a-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+(?:\\\\(\\\\s*(.+)\\\\s*\\" +
            "\\))(?:\\\\s+using\\\\s+([a-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+))?(?:\\\\s+with\\\\s+options\\\\" +
            "s*\\\\=\\\\s*(.+))?\\\\s*\\\\;?\",\r\n\t\t\t\t\"create\\\\s+materialized\\\\s+view\\\\s+(?:if\\\\s+not\\\\" +
            "s+exists\\\\s+)?([a-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+as\\\\s+select\\\\s+(.+)\\\\s+fro" +
            "m\\\\s+([a-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+where\\\\s+(.+)\\\\s+primary\\\\s+key\\\\s*(" +
            "?:\\\\(\\\\s*(.+)\\\\s*\\\\)\\\\s+with\\\\s+(.+)|\\\\(\\\\s*(.+)\\\\s*\\\\))\\\\s*\\\\;?\",\r\n            " +
            "                                                   \"create\\\\s+trigger\\\\s+(?:if\\\\" +
            "s+not\\\\s+exists\\\\s+)?([a-z0-9\\\\-_$%+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+on\\\\s+([a-z0-9\\\\-_$%" +
            "+=@!?<>^*&.\\\\\'\\\\\\\"]+)\\\\s+using\\\\s+(.+)\\\\s*\\\\;?\"\r\n\t\t\t]}\r\n}")]
        public string DiagnosticFileRegExAssocations {
            get {
                return ((string)(this["DiagnosticFileRegExAssocations"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<ArrayOfString xmlns:xsi=\"http://www.w3." +
            "org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\r\n  <s" +
            "tring>password</string>\r\n  <string>monitored_cassandra_pass</string>\r\n</ArrayOfS" +
            "tring>")]
        public global::System.Collections.Specialized.StringCollection ObscureFiledValues {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["ObscureFiledValues"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"[
{""Item1"":""SimpleSnitch"",""Item2"":null},
{""Item1"":""RackInferringSnitch"",""Item2"":null},
{""Item1"":""PropertyFileSnitch"",""Item2"":""cassandra-topology.properties""},
{""Item1"":""GossipingPropertyFileSnitch"",""Item2"":""cassandra-rackdc.properties""},
{""Item1"":""CloudstackSnitch"",""Item2"":null},
{""Item1"":""GoogleClouldSnitch"",""Item2"":""cassandra-rackdc.properties""},
{""Item1"":""EC2Snitch"",""Item2"":""cassandra-rackdc.properties""},
{""Item1"":""EC2MultiRegionSnitch"",""Item2"":""cassandra-rackdc.properties""},
{""Item1"":""YamlFileNetworkTopologySnitch"",""Item2"":""cassandra-topology.yaml""}
]")]
        public string SnitchFileMappings {
            get {
                return ((string)(this["SnitchFileMappings"]));
            }
        }
    }
}

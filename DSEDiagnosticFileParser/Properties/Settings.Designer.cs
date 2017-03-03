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
            "essingTaskOption\":\"IgnoreNode,OnlyOnce\", \"ProcessPriorityLevel\":1000},\r\n{\"Catago" +
            "ry\": \"CommandOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\status\"], \"Fi" +
            "leParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0," +
            " \"ProcessingTaskOption\":\"IgnoreNode,OnlyOnce\", \"ProcessPriorityLevel\":950},\r\n{\"C" +
            "atagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\opscenterd\\\\node_info.json\"]," +
            " \"FileParsingClass\": \"DSEDiagnosticFileParser.json_node_info\", \"NodeIdPos\": -1, " +
            "\"ProcessingTaskOption\":\"AllNodesInDataCenter\",\"ProcessPriorityLevel\":900},\r\n{\"Ca" +
            "tagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\opscenterd\\\\repair_service.jso" +
            "n\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_repair_service\", \"NodeIdP" +
            "os\": -1, \"ProcessingTaskOption\":\"AllNodesInDataCenter\",\"ProcessPriorityLevel\":90" +
            "0},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\machine-inf" +
            "o.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_machine_info\", \"Node" +
            "IdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPri" +
            "orityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\" +
            "*\\\\os-info.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_os_info_inf" +
            "o\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"P" +
            "rocessPriorityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\"." +
            "\\\\nodes\\\\*\\\\cpu.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_cpu_in" +
            "fo\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"" +
            "ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\"" +
            ".\\\\nodes\\\\*\\\\load_avg.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_" +
            "load_avg\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessi" +
            "ng\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"CommandOutputFile\", \"FilePatter" +
            "ns\": [\".\\\\nodes\\\\*\\\\agent_version.json\"], \"FileParsingClass\": \"DSEDiagnosticFile" +
            "Parser.file_agent_version\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"ScanForNode" +
            ",ParallelProcessing\", \"ProcessPriorityLevel\":100},\r\n{\"Catagory\": \"SystemOutputFi" +
            "le\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\java_system_properties.json\"], \"FileParsingC" +
            "lass\": \"DSEDiagnosticFileParser.json_java_system_properties\", \"NodeIdPos\": -1, \"" +
            "ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPriorityLevel\":1" +
            "00},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\ntpstat\"]," +
            " \"FileParsingClass\": \"DSEDiagnosticFileParser.file_ntpstat\", \"NodeIdPos\": -1, \"P" +
            "rocessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPriorityLevel\":10" +
            "0},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\ntptime\"], " +
            "\"FileParsingClass\": \"DSEDiagnosticFileParser.file_ntptime\", \"NodeIdPos\": -1, \"Pr" +
            "ocessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPriorityLevel\":100" +
            "},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\java_heap.js" +
            "on\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json-java_heap\", \"NodeIdPos\":" +
            " -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPriorityLe" +
            "vel\":100},\r\n{\"Catagory\": \"SystemOutputFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\memo" +
            "ry.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json-memory\", \"NodeIdPos" +
            "\": -1, \"ProcessingTaskOption\":\"ScanForNode,ParallelProcessing\", \"ProcessPriority" +
            "Level\":100},\r\n{\"Catagory\": \"LogFile\", \"FilePatterns\": [\".\\\\nodes\\\\*\\\\logs\\\\cassa" +
            "ndra\\\\system.log\"], \"FileParsingClass\": \"UserQuery.TestClass\", \"NodeIdPos\": -1}\r" +
            "\n]")]
        public string ProcessFileMappings {
            get {
                return ((string)(this["ProcessFileMappings"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"{
""file_nodetool_status"":{""RegExStrings"":[""datacenter:\\s+([a-z0-9\\-_$%+=@!?<>^*&]+)\\s*"",""(un|ul|uj|um|dn|dl|dJ|dm)\\s+([a-z0-9.:_\\-+%]+)\\s+([0-9.]+)\\s*([0-9a-z]{0,2})\\s+(\\d+)\\s+([0-9?.% ]+)\\s+([0-9a-f\\-]+)\\s+(.+)""]},
""file_ntpstat"":{""RegExString"":[""synchronised to NTP server\\s+\\((.+)\\).+stratum\\s+(\\d+).+time correct.+within\\s+(\\d+\\s+[a-zA-Z]+).+polling.+every\\s+(\\d+\\s+[a-zA-Z]+)""]},
""file_ntptime"":{""RegExString"":[""ntp_adjtime.+frequency\\s+([0-9\\-.]+\\s+[a-zA-Z]+)\\,\\s+interval\\s+([0-9\\-.]+\\s+[a-zA-Z]+)\\,\\s+maximum error\\s+([0-9\\-.]+\\s+[a-zA-Z]+)\\,\\s+estimated error\\s+([0-9\\-.]+\\s+[a-zA-Z]+)\\,.+time constant\\s+([0-9\\-.]+)\\,\\s+precision\\s+([0-9\\-.]+\\s+[a-zA-Z]+)\\,\\s+tolerance\\s+([0-9]+\\s+[a-zA-Z]+)"",""ntp_gettime.+maximum error\\s+([0-9\\-.]+\\s+[a-zA-Z]+)\\,\\s+estimated error\\s+([0-9\\-.]+\\s+[a-zA-Z]+)""]}
}")]
        public string DiagnosticFileRegExAssocations {
            get {
                return ((string)(this["DiagnosticFileRegExAssocations"]));
            }
        }
    }
}

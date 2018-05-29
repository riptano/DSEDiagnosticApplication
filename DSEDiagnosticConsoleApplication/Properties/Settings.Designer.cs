﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace DSEDiagnosticConsoleApplication.Properties {
    
    
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.VisualStudio.Editors.SettingsDesigner.SettingsSingleFileGenerator", "15.7.0.0")]
    internal sealed partial class Settings : global::System.Configuration.ApplicationSettingsBase {
        
        private static Settings defaultInstance = ((Settings)(global::System.Configuration.ApplicationSettingsBase.Synchronized(new Settings())));
        
        public static Settings Default {
            get {
                return defaultInstance;
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("[Personal]\\DSEData\\HL_Statim_Cluster-diagnostics-2016_09_12_14_39_32_UTC")]
        public string DiagnosticPath {
            get {
                return ((string)(this["DiagnosticPath"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("OpsCtrDiagStruct")]
        public string DiagFolderStruct {
            get {
                return ((string)(this["DiagFolderStruct"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@"<?xml version=""1.0"" encoding=""utf-16""?>
<ArrayOfString xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
  <string>dse_system</string>
  <string>system_auth</string>
  <string>system_distributed</string>
  <string>system_schema</string>
  <string>system</string>
  <string>dse_security</string>
  <string>solr_admin</string>
  <string>dse_auth</string>
  <string>dse_leases</string>
  <string>system_traces</string>
  <string>dse_perf</string>
  <string>HiveMetaStore</string>
  <string>cfs_archive</string>
  <string>cfs</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection IgnoreKeySpaces {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["IgnoreKeySpaces"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("")]
        public string ExcelFilePath {
            get {
                return ((string)(this["ExcelFilePath"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("^(?<CLUSTERNAME>.+)-diagnostics-(?<TS>\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2})_(?<TZ>" +
            "\\w{3})$")]
        public string OpsCenterDiagFolderRegEx {
            get {
                return ((string)(this["OpsCenterDiagFolderRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("yyyy_MM_dd_HH_mm_ss")]
        public string OpsCenterDiagFolderDateTimeFmt {
            get {
                return ((string)(this["OpsCenterDiagFolderDateTimeFmt"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("^(?<STARTTS>[0-9 \\-:./+]+)?\\s*(?<STARTTZ>[A-Z0-9_/]+)?\\s*,?\\s*(?<ENDTS>[0-9 \\-:./" +
            "+]+)?\\s*(?<ENDTZ>[A-Z0-9_/]+)?$")]
        public string CLParserLogTimeRangeRegEx {
            get {
                return ((string)(this["CLParserLogTimeRangeRegEx"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("168")]
        public int OnlyIncludeXHrsofLogsFromDiagCaptureTime {
            get {
                return ((int)(this["OnlyIncludeXHrsofLogsFromDiagCaptureTime"]));
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
  <string>system.batches</string>
  <string>system.hints</string>
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection WarnWhenKSTblIsDetected {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["WarnWhenKSTblIsDetected"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(".\\dseHealthAssessment3Template.xlsm")]
        public string ExcelFileTemplatePath {
            get {
                return ((string)(this["ExcelFileTemplatePath"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("{0}\\{1}-{2:yyyy-MM-dd-HH-mm-ss}-{3}{4}{5}")]
        public string ExcelFileNameGeneratedStringFormat {
            get {
                return ((string)(this["ExcelFileNameGeneratedStringFormat"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("Validate")]
        public string DefaultProfile {
            get {
                return ((string)(this["DefaultProfile"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("[\r\n{\"ProfileName\":\"AllFilesLogs\",\"Log4NetParser\":\".\\\\Json\\\\Log4NetParser.json\",\"P" +
            "rocessFileMappings\":\".\\\\Json\\\\ProcessFileMappings.json\",\"EnableVirtualMemory\":tr" +
            "ue, \"DefaultLogLevelHandling\":\"Warning, Error, Fatal, Exception\", \"DebugLogProce" +
            "ssingTypes\":\"OnlyFlushCompactionMsgs\"},\r\n{\"ProfileName\":\"NoLogs\",\"Log4NetParser\"" +
            ":\".\\\\Json\\\\Log4NetParser.json\",\"ProcessFileMappings\":\".\\\\Json\\\\ProcessFileMappin" +
            "gsNoLogs.json\",\"EnableVirtualMemory\":true, \"DefaultLogLevelHandling\":null, \"Debu" +
            "gLogProcessingTypes\":\"Disabled\"},\r\n{\"ProfileName\":\"Validate\",\"Log4NetParser\":\".\\" +
            "\\Json\\\\Log4NetParserValidateLogs.json\",\"ProcessFileMappings\":\".\\\\Json\\\\ProcessFi" +
            "leMappingsValidate.json\",\"EnableVirtualMemory\":false, \"DefaultLogLevelHandling\":" +
            "\"Fatal, Exception\", \"DebugLogProcessingTypes\":\"OnlyLogDateRange\"},\r\n{\"ProfileNam" +
            "e\":\"Decompression\",\"Log4NetParser\":\".\\\\Json\\\\Log4NetParserValidateLogs.json\",\"Pr" +
            "ocessFileMappings\":\".\\\\Json\\\\Unzip-ProcessFileMappings.json\",\"EnableVirtualMemor" +
            "y\":false, \"DefaultLogLevelHandling\":\"Fatal, Exception\", \"DebugLogProcessingTypes" +
            "\":\"Disabled\"},\r\n{\"ProfileName\":\"CreateOpsCenterStruct\",\"Log4NetParser\":\".\\\\Json\\" +
            "\\Log4NetParserValidateLogs.json\",\"ProcessFileMappings\":\".\\\\Json\\\\ProcessFileMapp" +
            "ings-CreateOpsCenterCopy.json\",\"EnableVirtualMemory\":false, \"DefaultLogLevelHand" +
            "ling\":\"Fatal, Exception\", \"DebugLogProcessingTypes\":\"Disabled\"},\r\n{\"ProfileName\"" +
            ":\"AllNoFlushComp\",\"Log4NetParser\":\".\\\\Json\\\\Log4NetParserLogsNoFlushComp.json\",\"" +
            "ProcessFileMappings\":\".\\\\Json\\\\ProcessFileMappingsNoDebugLogs.json\",\"EnableVirtu" +
            "alMemory\":true, \"DefaultLogLevelHandling\":\"Warning, Error, Fatal, Exception\", \"D" +
            "ebugLogProcessingTypes\":\"Disable\"},\r\n{\"ProfileName\":\"ValidateWLogs\",\"Log4NetPars" +
            "er\":\".\\\\Json\\\\Log4NetParserLogsNoFlushCompGC.json\",\"ProcessFileMappings\":\".\\\\Jso" +
            "n\\\\ProcessFileMappingsValidate.json\",\"EnableVirtualMemory\":false, \"DefaultLogLev" +
            "elHandling\":\"Fatal, Exception\", \"DebugLogProcessingTypes\":\"OnlyLogDateRange\"}\r\n]" +
            "")]
        public string Profiles {
            get {
                return ((string)(this["Profiles"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("00:30:00")]
        public global::System.TimeSpan LogAggregationPeriod {
            get {
                return ((global::System.TimeSpan)(this["LogAggregationPeriod"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("")]
        public string IgnoreLogParsingTagEvents {
            get {
                return ((string)(this["IgnoreLogParsingTagEvents"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(".\\*\\*.log.*")]
        public string AppendFilePathForAddLogArgument {
            get {
                return ((string)(this["AppendFilePathForAddLogArgument"]));
            }
        }
    }
}

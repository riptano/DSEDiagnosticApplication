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
</ArrayOfString>")]
        public global::System.Collections.Specialized.StringCollection IgnoreKeySpaces {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["IgnoreKeySpaces"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("[DeskTop]\\testa.xlsx")]
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
    }
}

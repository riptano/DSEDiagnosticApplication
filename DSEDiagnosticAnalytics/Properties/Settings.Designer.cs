﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace DSEDiagnosticAnalytics.Properties {
    
    
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("Microsoft.VisualStudio.Editors.SettingsDesigner.SettingsSingleFileGenerator", "15.8.0.0")]
    internal sealed partial class Settings : global::System.Configuration.ApplicationSettingsBase {
        
        private static Settings defaultInstance = ((Settings)(global::System.Configuration.ApplicationSettingsBase.Synchronized(new Settings())));
        
        public static Settings Default {
            get {
                return defaultInstance;
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("4")]
        public double LogFileInfoAnalysisContinousEventInDays {
            get {
                return ((double)(this["LogFileInfoAnalysisContinousEventInDays"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("15")]
        public double LogFileInfoAnalysisGapTriggerInMins {
            get {
                return ((double)(this["LogFileInfoAnalysisGapTriggerInMins"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute(@".\AggregateGroups.json,.\Json\AggregateGroups.json,[cpld]\Json\AggregateGroups.json,[cpld]\AggregateGroups.json,[ApplicationRunTimeDirectory]\Json\AggregateGroups.json,[ApplicationRunTimeDirectory]\AggregateGroups.json,.\Json\AggregateGroups.json,.\AggregateGroups.json")]
        public string AggregateGroups {
            get {
                return ((string)(this["AggregateGroups"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("Partition large")]
        public string PartitionLargeAttrib {
            get {
                return ((string)(this["PartitionLargeAttrib"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("Tombstones Read")]
        public string TombstonesReadAttrib {
            get {
                return ((string)(this["TombstonesReadAttrib"]));
            }
        }
        
        [global::System.Configuration.UserScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("Tombstone/Live Percent")]
        public string TombstoneLiveCellRatioAttrib {
            get {
                return ((string)(this["TombstoneLiveCellRatioAttrib"]));
            }
            set {
                this["TombstoneLiveCellRatioAttrib"] = value;
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("Compaction Insufficient Space")]
        public string CompactionInsufficientSpace {
            get {
                return ((string)(this["CompactionInsufficientSpace"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("Row large")]
        public string RowLargeAttrrib {
            get {
                return ((string)(this["RowLargeAttrrib"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("00:05:00")]
        public global::System.TimeSpan GCTimeFrameDetection {
            get {
                return ((global::System.TimeSpan)(this["GCTimeFrameDetection"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("150")]
        public double GCBackToBackToleranceMS {
            get {
                return ((double)(this["GCBackToBackToleranceMS"]));
            }
        }
        
        [global::System.Configuration.ApplicationScopedSettingAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Configuration.DefaultSettingValueAttribute("1.25")]
        public double GCTimeFrameDetectionThreholdMin {
            get {
                return ((double)(this["GCTimeFrameDetectionThreholdMin"]));
            }
        }
    }
}

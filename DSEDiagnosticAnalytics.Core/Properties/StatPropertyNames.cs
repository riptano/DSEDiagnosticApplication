
namespace DSEDiagnosticAnalytics.Properties {
    
    
    public sealed class StatPropertyNames
    {
        private static System.Lazy<StatPropertyNames> DefaultInstance = new System.Lazy<StatPropertyNames>(() => new StatPropertyNames());

        public static StatPropertyNames Default
        {
            get
            {
                return DefaultInstance.Value;
            }
        }

        public object this[string propertyName]
        {
            get
            {
                return DSEDiagnosticAnalytics.Properties.Settings.Default[propertyName];
            }
        }

        public string SSTableOverallPercent {
            get {
                return ((string)(this["SSTableOverallPercent"]));
            }
        }
                
        public string TombstoneOverallPercent {
            get {
                return ((string)(this["TombstoneOverallPercent"]));
            }
        }
               
        public string ReadOverallPercent {
            get {
                return ((string)(this["ReadOverallPercent"]));
            }
        }
               
        public string WriteOverallPercent {
            get {
                return ((string)(this["WriteOverallPercent"]));
            }
        }
               
        public string KeysOverallPercent {
            get {
                return ((string)(this["KeysOverallPercent"]));
            }
        }
               
        public string SolrIndexStorageSizePercent {
            get {
                return ((string)(this["SolrIndexStorageSizePercent"]));
            }
        }
                
        public string TotalStorage {
            get {
                return ((string)(this["TotalStorage"]));
            }
        }
                
        public string LocalReadCount {
            get {
                return ((string)(this["LocalReadCount"]));
            }
        }
               
        public string LocalWriteCount {
            get {
                return ((string)(this["LocalWriteCount"]));
            }
        }
               
        public string LCSLevels {
            get {
                return ((string)(this["LCSLevels"]));
            }
        }
               
        public string TombstoneLiveCellRatio {
            get {
                return ((string)(this["TombstoneLiveCellRatio"]));
            }
        }
                
        public string LocalReadPercent {
            get {
                return ((string)(this["LocalReadPercent"]));
            }
        }
    
        public string LocalWritePercent {
            get {
                return ((string)(this["LocalWritePercent"]));
            }
        }
              
        public string LCSBackRatio {
            get {
                return ((string)(this["LCSBackRatio"]));
            }
        }
       
        public string LCSSplitLevels {
            get {
                return ((string)(this["LCSSplitLevels"]));
            }
        }
               
        public string LCSNbrSplitLevels {
            get {
                return ((string)(this["LCSNbrSplitLevels"]));
            }
        }
               
        public string SolrIndexStorageSize {
            get {
                return ((string)(this["SolrIndexStorageSize"]));
            }
        }
               
        public string PartitionLarge {
            get {
                return ((string)(this["PartitionLarge"]));
            }
        }
               
        public string TombstonesRead {
            get {
                return ((string)(this["TombstonesRead"]));
            }
        }
    
        public string CompactionInsufficientSpace {
            get {
                return ((string)(this["CompactionInsufficientSpace"]));
            }
        }
              
        public string RowLarge {
            get {
                return ((string)(this["RowLarge"]));
            }
        }
                
        public string TombstoneLiveCell {
            get {
                return ((string)(this["TombstoneLiveCell"]));
            }
        }
    
        public string LocalReadLatency {
            get {
                return ((string)(this["LocalReadLatency"]));
            }
        }
       
        public string LocalWriteLatency {
            get {
                return ((string)(this["LocalWriteLatency"]));
            }
        }
        
       
        public string AvgTombstonesSlice {
            get {
                return ((string)(this["AvgTombstonesSlice"]));
            }
        }
        
       
        public string MaxTombstonesSlice {
            get {
                return ((string)(this["MaxTombstonesSlice"]));
            }
        }
        
      
        public string CompactedPartitionMin {
            get {
                return ((string)(this["CompactedPartitionMin"]));
            }
        }
        
       
        public string CompactedPartitionMax {
            get {
                return ((string)(this["CompactedPartitionMax"]));
            }
        }
        
       
        public string CompactedPartitionAvg {
            get {
                return ((string)(this["CompactedPartitionAvg"]));
            }
        }
        
       
        public string AvgLiveCellsReadSlice {
            get {
                return ((string)(this["AvgLiveCellsReadSlice"]));
            }
        }
        
       
        public string MaxLiveCellsReadSlice {
            get {
                return ((string)(this["MaxLiveCellsReadSlice"]));
            }
        }
        
        
        public string SSTableCount {
            get {
                return ((string)(this["SSTableCount"]));
            }
        }
        
      
        public string ReadCount {
            get {
                return ((string)(this["ReadCount"]));
            }
        }
        
       
        public string WriteCount {
            get {
                return ((string)(this["WriteCount"]));
            }
        }
        
        
        public string LocalWriteCout {
            get {
                return ((string)(this["LocalWriteCout"]));
            }
        }
        
       
        public global::System.Collections.Specialized.StringCollection NbrPartitionKeys {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["NbrPartitionKeys"]));
            }
        }
        
       
        public global::System.Collections.Specialized.StringCollection PartitionRowLarge {
            get {
                return ((global::System.Collections.Specialized.StringCollection)(this["PartitionRowLarge"]));
            }
        }
        
       
        public string CommonPartitionKeys {
            get {
                return ((string)(this["CommonPartitionKeys"]));
            }
        }
        
       
        public string LogLargePartition {
            get {
                return ((string)(this["LogLargePartition"]));
            }
        }
        
       
        public string LogTombstones {
            get {
                return ((string)(this["LogTombstones"]));
            }
        }
        
       
        public string LogLiveCells {
            get {
                return ((string)(this["LogLiveCells"]));
            }
        }
        
        
        public string LogInsufficientSpace {
            get {
                return ((string)(this["LogInsufficientSpace"]));
            }
        }
        
       
        public string LogInsufficientSpaceSubClass {
            get {
                return ((string)(this["LogInsufficientSpaceSubClass"]));
            }
        }
    }
}

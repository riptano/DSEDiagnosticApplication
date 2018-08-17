using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_nodetool_proxyhistograms : DiagnosticFile
    {

        public file_nodetool_proxyhistograms(CatagoryTypes catagory,
                                                IDirectoryPath diagnosticDirectory,
                                                IFilePath file,
                                                INode node,
                                                string defaultClusterName,
                                                string defaultDCName,
                                                Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new StatResults(this);
        }

        public file_nodetool_proxyhistograms(IFilePath file,
                                                string defaultClusterName,
                                                string defaultDCName,
                                                Version targetDSEVersion = null)
            : base(CatagoryTypes.CommandOutputFile, file, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new StatResults(this);
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class StatResults : IResult
        {
            private StatResults() { }
            internal StatResults(file_nodetool_proxyhistograms fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this._statsList = fileInstance._statsList;
            }

            [JsonProperty(PropertyName = "Stats")]
            private readonly List<AggregatedStats> _statsList;

            #region IResult
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; private set; }
            [JsonIgnore]
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            [JsonIgnore]
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get { return this._statsList.Count; } }

            [JsonIgnore]
            public IEnumerable<IParsed> Results { get { return this._statsList; } }

            #endregion
        }

        [JsonProperty(PropertyName = "Results")]
        private readonly StatResults _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        private readonly List<AggregatedStats> _statsList = new List<AggregatedStats>();

        /*
         proxy histograms
        Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                         (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
        50%                    182.79             219.34               0.00               0.00               0.00               0.00
        75%                    545.79             315.85               0.00               0.00               0.00               0.00
        95%                   3379.39             785.94               0.00               0.00               0.00               0.00
        98%                   8409.01            1629.72               0.00               0.00               0.00               0.00
        99%                  10090.81            2816.16               0.00               0.00               0.00               0.00
        Min                     29.52              24.60               0.00               0.00               0.00               0.00
        Max                  25109.16           52066.35               0.00               0.00               0.00               0.00
         */

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();

            string line;
            Match regExMatch;
            bool nextLineUOM = false;
            var uoms = new string[] { "micros", "micros", "micros", "micros", "micros", "micros"};
            var attrNames = new string[] { "Latency.Read", "Latency.Write", "Latency.Range", "Latency.Read.CAS", "Latency.Write.CAS", "Latency.Write.View" };
            bool skipNextLine = false;
            
            var statItem = new AggregatedStats(this.File,
                                                this.Node,
                                                SourceTypes.Histogram,
                                                EventTypes.AggregateDataTool,
                                                EventClasses.NodeStats);
            var checkValue = (Action<string, string, string>)((latName, capValue, uom) =>
            {
                if (string.IsNullOrEmpty(capValue) || capValue == "N/A")
                {
                    statItem.AssociateItem(latName, UnitOfMeasure.NaNValue);
                }
                else
                {
                    statItem.AssociateItem(latName, new UnitOfMeasure(capValue, uom, UnitOfMeasure.Types.Time));
                }
            });

            this._statsList.Add(statItem);

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                if (skipNextLine)
                {
                    skipNextLine = false;
                    continue;
                }

                line = element.Trim();

                if (line == string.Empty) continue;
                if (line == "proxy histograms") continue;
                if (line.StartsWith("percentile", StringComparison.OrdinalIgnoreCase))
                {
                    nextLineUOM = true;
                    continue;
                }

                ++this.NbrItemsParsed;

                if (nextLineUOM)
                {
                    nextLineUOM = false;

                    regExMatch = this.RegExParser.Match(line, 0);

                    if(regExMatch.Success)
                    {
                        for(int nIdx = 1; nIdx < regExMatch.Groups.Count; ++nIdx)
                        {
                            if (regExMatch.Groups[nIdx].Success)
                                uoms[nIdx] = regExMatch.Groups[nIdx].Value;
                        }
                        continue;
                    }
                }
                
                regExMatch = this.RegExParser.Match(line, 1);

                if (regExMatch.Success)
                {
                    var percentile = regExMatch.Groups[1].Value;

                    for(int nIdx = 2; nIdx < regExMatch.Groups.Count; ++nIdx)
                    {
                        if(regExMatch.Groups[nIdx].Success)
                            checkValue(attrNames[nIdx - 2] + '.' + percentile, regExMatch.Groups[nIdx].Value, uoms[nIdx-2]);
                    }
                }
                else
                {
                    Logger.Instance.ErrorFormat("FileMapper<{3}>\t{0}\t{1}\tInvalid Line \"{2}\" found in nodetool proxyhistograms File.",
                                                this.Node,
                                                this.ShortFilePath,
                                                line,
                                                this.MapperId);
                    ++this.NbrErrors;
                }
            }

            this.Processed = true;
            return (uint)this._statsList.Count;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Patterns.Tasks;

namespace DSEDiagnosticConsoleApplication
{
    partial class Program
    {

        static Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> ProcessDSEDiagnosticFileParser(System.Threading.CancellationTokenSource cancellationSource)
        {
            string defaultCluster = null;

            if (ParserSettings.DiagFolderStruct == ParserSettings.DiagFolderStructOptions.OpsCtrDiagStruct
                    && !DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp.HasValue)
            {
                var regEx = new System.Text.RegularExpressions.Regex(Properties.Settings.Default.OpsCenterDiagFolderRegEx,
                                                                        System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                var regexMatch = regEx.Match(ParserSettings.DiagnosticPath.Name);

                if (regexMatch.Success)
                {
                    var clusterName = regexMatch.Groups["CLUSTERNAME"].Value;
                    var diagTS = regexMatch.Groups["TS"].Value;
                    var diagTZ = regexMatch.Groups["TZ"].Value;
                    DateTime diagDateTime;

                    //defaultCluster = clusterName.Trim(); //Turns out OpsCenter translate "-" into "_" for cluster name in the folder name and there is no way of knowing if it has been translated
                    if (DateTime.TryParseExact(diagTS,
                                                Properties.Settings.Default.OpsCenterDiagFolderDateTimeFmt,
                                                System.Globalization.CultureInfo.InvariantCulture,
                                                System.Globalization.DateTimeStyles.None,
                                                out diagDateTime))
                    {
                        DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp = diagDateTime.ConvertToOffSet(diagTZ);
                    }

                    Logger.Instance.InfoFormat("Using Diagnostic Tar-Ball \"{0}\" with OpsCenter Capture Date of {1}",
                                                    clusterName,
                                                    DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp.HasValue
                                                        ? DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp.Value.ToString(@"yyyy-MM-dd HH:mm:ss zzz")
                                                        : "<Unkown>");
                }
            }

            if (ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime > 0
                && DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp.HasValue)
            {
                DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange = new DateTimeOffsetRange(DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp.Value - TimeSpan.FromHours(ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime), DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp.Value);
            }

            if (DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange != null)
            {
                Logger.Instance.WarnFormat("Using UTC Log Range of {0} to {1}",
                                                DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.Min == DateTimeOffset.MinValue
                                                            ? "MinValue"
                                                            : DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.Min.ToString(@"yyyy-MM-dd HH:mm:ss zzz"),
                                                 DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.Max == DateTimeOffset.MaxValue
                                                            ? "MaxValue"
                                                            : DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange.Max.ToString(@"yyyy-MM-dd HH:mm:ss zzz"));
            }

            DSEDiagnosticFileParser.LibrarySettings.IgnoreWarningsErrosInKeySpaces = ParserSettings.IgnoreKeySpaces;
            
            var diagParserTask = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(ParserSettings.DiagnosticPath,
                                                                                     clusterName: ParserSettings.ClusterName ?? defaultCluster,
                                                                                     clusterHashCode: ParserSettings.ClusterHashCode,
                                                                                     dseVersion: ParserSettings.DSEVersion,
                                                                                     cancellationSource: cancellationSource,
                                                                                     additionalFilesForClass: ParserSettings.AdditionalFilesForParsingClass,
                                                                                     onlyNodes: ParserSettings.OnlyNodes == null || ParserSettings.OnlyNodes.Count == 0
                                                                                                    ? null
                                                                                                    : ParserSettings.OnlyNodes
                                                                                                        .Select(n => DSEDiagnosticLibrary.NodeIdentifier.Create(n)));

            diagParserTask.Then(ignore => { ConsoleTasksReadFiles.Terminate(); ConsoleLogReadFiles.Terminate(); ConsoleDeCompressFiles.Terminate(); });
            diagParserTask.Then(ignore =>
            {
                foreach (var uaNode in DSEDiagnosticLibrary.Cluster.GetUnAssocaitedNodes())
                {
                    if (uaNode.DataCenter == null || uaNode.DataCenter is DSEDiagnosticLibrary.PlaceholderDataCenter)
                    {
                        Logger.Instance.WarnFormat("Node \"{0}\" in DC \"{1}\" was detected as being Orphaned. This may indicated a processing error!", uaNode, uaNode.DataCenter);
                    }
                }
            });

            diagParserTask.ContinueWith(task => CanceledFaultProcessing(task.Exception),
                                            TaskContinuationOptions.OnlyOnFaulted);
            diagParserTask.ContinueWith(task => CanceledFaultProcessing(null),
                                            TaskContinuationOptions.OnlyOnCanceled);

            return diagParserTask;
        }
    }
}

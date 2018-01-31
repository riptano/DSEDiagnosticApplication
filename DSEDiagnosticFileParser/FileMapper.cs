using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;
using System.Threading;

namespace DSEDiagnosticFileParser
{
	public sealed class FileMapper
	{
        [Flags]
        public enum ProcessingTaskOptions
        {
            None = 0,
            /// <summary>
            /// Determine Node by scanning the File Path
            /// </summary>
            ScanForNode = 0x0001,
            /// <summary>
            /// Process this file using all nodes defined within a data center
            /// Note that if the default data center name is not defined or not provided, this is the same as AllNodesInCluster.
            /// </summary>
            AllNodesInDataCenter = 0x0002,
            /// <summary>
            /// Process this file using all nodes defined within a cluster
            /// Note that if the default cluster name is not defined or not provided, the Local Master cluster is used.
            /// </summary>
            AllNodesInCluster = 0x0004,
            /// <summary>
            /// If defined, the file is process regardless if a node can be determined. 
            /// </summary>
            IgnoreNode = 0x0008,
            /// <summary>
            /// If defined, this file processing task will be ran in parallel with other files processing tasks within this ProcessPriority level. All parallel tasks within this level must complete before moving onto the next level.
            /// If this is not defined, all non-parallel processing tasks will be executed sequentially within this level and than all parallel defined processing tasks will be executed in parallel.
            /// </summary>
            /// <remarks>
            /// Lower process priorities will be block until all file processing within this level completes unless AllowLowerPriorityProcessingToStart is defined.
            /// </remarks>
            ParallelProcessingWithinPriorityLevel = 0x0100,
            /// <summary>
            /// If defined, this file processing task will be ran in parallel as soon as this task level&apos;s non-parallel tasks are executed.
            /// If this is not defined, all non-parallel processing tasks will be executed sequentially within this level and than all parallel defined processing tasks will be executed in parallel.
            /// </summary>
            ParallelProcessing = 0x0010,
            /// <summary>
            /// If defined and this task is still executing and no other tasks at this level are running without this defined, lower tasks will be able to start.
            /// If not defined, lower tasks will be blocked until this task completes.
            /// </summary>
            AllowLowerPriorityProcessingToStart = 0x0020,
            /// <summary>
            /// If defined all files found that match FilePatterns are processed in parallel.
            /// Note this option is ignored if OnlyOnce is defined.
            /// </summary>
            ParallelProcessFiles = 0x0040,
            /// <summary>
            /// If defined, nodes' files are parallel process. ScanForNode is also defined.
            /// Note this option is ignored if OnlyOnce is defined.
            /// </summary>
            ParallelProcessNode = 0x0080 | ScanForNode,
            /// <summary>
            /// As soon as the first file is successfully processed, all reminding file to be process are canceled.
            /// Warning: ParallelProcessFiles is ignored (all files are processed synchronous).
            /// </summary>
            OnlyOnce = 0x0200,
            /// <summary>
            /// If defined files are sorted by the file's timestamp where the oldest files are first, the default is most current file is first.
            /// Note: in cases where the same path name is detected, the sort order will determine which file is actually selected. If SortFilesByOldest is used the oldest file is selected.
            /// If the default sort order is used the most current file is used.
            /// </summary>
            SortFilesByOldest = 0x0400,

            Default = ScanForNode | ParallelProcessingWithinPriorityLevel | ParallelProcessFiles
        }

        public static ProcessingTaskOptions DetermineParallelOptions(ProcessingTaskOptions taskOptions)
        {
            return taskOptions.HasFlag(ProcessingTaskOptions.ParallelProcessing)
                    ? ProcessingTaskOptions.ParallelProcessing
                    : (taskOptions.HasFlag(ProcessingTaskOptions.ParallelProcessingWithinPriorityLevel)
                            ? ProcessingTaskOptions.ParallelProcessingWithinPriorityLevel
                            : ProcessingTaskOptions.None);
        }

        public FileMapper()
        {}

        public FileMapper(DiagnosticFile.CatagoryTypes catagory,
                            string fileParsingClass,
                            int? nodeIdPos,
                            ProcessingTaskOptions? processingOptions,
                            params string [] filePatterns)
        {
            this.NodeIdPos = nodeIdPos.HasValue ? nodeIdPos.Value : -1;
            this.Catagory = catagory;
            this.FileParsingClass = fileParsingClass;
            this.ProcessingTaskOption = processingOptions.HasValue ? processingOptions.Value : ProcessingTaskOptions.Default;
            this.FilePatterns = filePatterns;
        }

        public FileMapper(DiagnosticFile.CatagoryTypes catagory,
                            string fileParsingClass,
                            int? nodeIdPos,
                            ProcessingTaskOptions? processingOptions,
                            string version,
                            params string[] filePatterns)
            : this(catagory, fileParsingClass, nodeIdPos, processingOptions, filePatterns)
        {
           this.MatchVersion = new Version(version);
        }

        public DiagnosticFile.CatagoryTypes Catagory { get; set; }

		public string[] FilePatterns { get; set; }

        /// <summary>
        /// Ignore any files (includes complete path) that match this RegEx.
        /// </summary>
        public string IgnoreFilesMatchingRegEx { get; set; }
		public string FileParsingClass { get; set; }
        /// <summary>
        /// The directory level of the node&apos;s IP Address or host name as the directory name. The directory above the file is position 1. It can be embedded and can be either an IPAddress or host name (seperated by a space or plus sign).
        /// If 0, the file name is searched where the node&apos;s Ip Address or host name. This can be embeded within the file name. If a host name is used it must be at the beginning of the file name and seperated by a space or plus sign.
        /// If -1, the file name is first reviewed, than from the file each directory level is searched up to root. During the directory search only IPAddresses are being looked for. They can be embedded.
        /// </summary>
        public int NodeIdPos { get; set; } = -1;
        public ProcessingTaskOptions ProcessingTaskOption { get; set; } = ProcessingTaskOptions.Default;

        /// <summary>
        /// The processing task level. Higher numbers (levels) means this task will be processed sooner than lower numbers (levels).
        /// </summary>
        public short ProcessPriorityLevel { get; set; }

        public string DefaultDataCenter { get; set; }
        public string DefaultCluster { get; set;}

        /// <summary>
        /// If null any DSE version will match this mappings.
        /// If defined, any DSE version that is equal to or greater than this version will match this mapping.
        /// </summary>
        /// <example>
        /// Match Versions:
        ///     4.7.0
        ///     4.7.5
        ///     4.8.0
        ///     4.8.5
        ///     5.0.0
        /// DSE Version 4.8.12 -&gt; 4.8.5
        ///             4.8.4  -&gt; 4.8.0
        ///             4.8.0  -&gt; 4.8.0
        /// </example>
        public Version MatchVersion { get; set; }

        public CancellationTokenSource CancellationSource { get; set; }

        public bool Enabled { get; set; } = true;

        public IFilePath[] FilePathMerge(IDirectoryPath diagnosticDirectory)
		{
			if(this.FilePatterns == null)
			{
				return null;
			}

			return FilePatterns.Select(p => PathUtils.Parse(p, diagnosticDirectory.Path)).Cast<IFilePath>().ToArray();
		}

		public Type GetFileParsingType()
		{
			return string.IsNullOrEmpty(this.FileParsingClass) ? null : TypeHelpers.GetDataType(this.FileParsingClass);
		}
    }
}

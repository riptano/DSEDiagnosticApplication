using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;
using Common;
using Common.Path;
using System.IO;
using System.Threading;
using Newtonsoft.Json;

namespace DSEDiagnosticFileParser
{
    public abstract class DiagnosticFile
    {
		public enum CatagoryTypes
		{
			Unknown = 0,
			ConfigurationFile,
			LogFile,
            AchievedLogFile,
            CommandOutputFile,
			SystemOutputFile,
            CQLFile,
            ZipFile
		}

        #region Events

        /// <summary>
        ///     Called when an exception is caught and processed.
        /// </summary>
        public static event ExceptionEventArgs.EventHandler OnException;

        public static bool InvokeExceptionEvent(DiagnosticFile sender,
                                                    System.Exception exception,
                                                    System.Threading.CancellationTokenSource cancellationTokenSource,
                                                    object[] associatedObjects)
        {
            return ExceptionEventArgs.InvokeEvent(sender, exception, cancellationTokenSource, associatedObjects, OnException);
        }

        public static bool InvokeExceptionEvent(string sender,
                                                    System.Exception exception,
                                                    System.Threading.CancellationTokenSource cancellationTokenSource,
                                                    object[] associatedObjects)
        {
            return ExceptionEventArgs.InvokeEvent(sender, exception, cancellationTokenSource, associatedObjects, OnException);
        }

        #endregion //end of Events

        public static Dictionary<string, RegExParseString> RegExAssocations = LibrarySettings.DiagnosticFileRegExAssocations;
        private static bool _DisableParallelProcessing = false;
        public static bool DisableParallelProcessing
        {
            get { return _DisableParallelProcessing; }
            set
            {
                _DisableParallelProcessing = value;
                Logger.Instance.WarnFormat("DisableParallelProcessing: {0}", DisableParallelProcessing);
            }
        }

        protected DiagnosticFile(CatagoryTypes catagory,
									IDirectoryPath diagnosticDirectory,
									IFilePath file,
									INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
		{
			this.Catagory = catagory;
			this.DiagnosticDirectory = diagnosticDirectory;
			this.File = file;
			this.Node = node;
			this.ParsedTimeRange = new DateTimeRange();
			this.ParsedTimeRange.SetMinimal(DateTime.Now);
            this.DefaultClusterName = defaultClusterName;
            this.DefaultDataCenterName = defaultDCName;
            this.TargetDSEVersion = targetDSEVersion;

            this.RegExParser = RegExAssocations.TryGetValue(this.GetType().Name);
        }

        #region Public Members
        public CatagoryTypes Catagory { get; private set; }
		public IDirectoryPath DiagnosticDirectory { get; private set; }
		public IFilePath File { get; private set; }
		public INode Node { get; private set; }
        public string DefaultClusterName { get; private set; }
        public string DefaultDataCenterName { get; private set; }
        public DateTimeRange ParseOnlyInTimeRange { get; protected set; }
        public Version TargetDSEVersion { get; protected set; }
        public int NbrItemsParsed { get; protected set; }
		public DateTimeRange ParsedTimeRange { get; protected set; }
        /// <summary>
        /// True to indicate the process was sucessfully completed;
        /// </summary>
        public bool Processed { get; protected set; }
        public System.Exception Exception { get; protected set; }
        private int _nbrItemGenerated = 0;
        public int NbrItemGenerated { get { return this._nbrItemGenerated; } }
        public RegExParseString RegExParser { get; protected set; }
        public CancellationToken CancellationToken { get; set; }
        public bool Canceled { get; protected set; }
        public Task<DiagnosticFile> Task { get; protected set; }

        public IResult Result { get { return this.GetResult(); } }

        #endregion

		#region static

		/// <summary>
		///
		/// </summary>
		/// <param name="filePath"></param>
		/// <param name="nodeIdPos">
		/// The directory level of the node&apos;s IP Address or host name as the directory name. The directory above the file is position 1. It can be embedded and can be either an IPAddress or host name (seperated by a space or plus sign).
		/// If 0, the file name is searched where the node&apos;s Ip Address or host name. This can be embeded within the file name. If a host name is used it must be at the beginning of the file name and seperated by a space or plus sign.
		/// If -1, the file name is first reviewed, than from the file each directory level is searched up to root. During the directory search only IPAddresses are being looked for. They can be embedded.
		/// </param>
		/// <returns></returns>
		protected static NodeIdentifier DetermineNodeIdentifier(IFilePath filePath, int nodeIdPos)
		{
			NodeIdentifier nodeId = null;

			if(nodeIdPos <= 0)
			{
                nodeId = NodeIdentifier.CreateNodeIdentifer(filePath.FileNameWithoutExtension);
            }

            if (nodeId == null && nodeIdPos != 0)
			{
				bool scanning = nodeIdPos < 0;
				var directories = filePath.ParentDirectoryPath.PathResolved.Split(Path.DirectorySeparatorChar);

				if (scanning)
				{
					for (int dirLevel = directories.Length - 1; dirLevel >= 0; --dirLevel)
					{
						nodeId = NodeIdentifier.CreateNodeIdentifer(directories[dirLevel]);

						if(nodeId != null)
						{
							return nodeId;
						}
					}
				}
				else
				{
					nodeId = NodeIdentifier.CreateNodeIdentifer(directories[directories.Length - nodeIdPos]);
				}
			}

			return nodeId;
		}

        public static Task<IEnumerable<DiagnosticFile>> ProcessFile(IDirectoryPath diagnosticDirectory,
                                                                        string dataCenterName = null,
                                                                        string clusterName = null,
                                                                        Version dseVersion = null,
                                                                        CancellationTokenSource cancellationSource = null)
        {
            FileMapper filemapperInError = null;
            var diagFilesList = new List<DiagnosticFile>();
            var mappers = DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings
                            .OrderByDescending(o => o.ProcessPriorityLevel)
                            .ThenBy(o => DSEDiagnosticFileParser.FileMapper.DetermineParallelOptions(o.ProcessingTaskOption))
                            .GroupBy(k => new { ProcessPriorityLevel = k.ProcessPriorityLevel, ParallelProcessingWithinPriorityLevel = DSEDiagnosticFileParser.FileMapper.DetermineParallelOptions(k.ProcessingTaskOption) });
            int mapperId = 0;
            var parallelOptions = new ParallelOptions();

            if(cancellationSource != null) parallelOptions.CancellationToken = cancellationSource.Token;
            parallelOptions.MaxDegreeOfParallelism = System.Environment.ProcessorCount;

            foreach (var mapperGroup in mappers)
            {
                var fileMappings = mapperGroup.Where(m => m.MatchVersion == null).ToList();

                if(dseVersion != null)
                {
                    var versionedMapper = mapperGroup.Where(m => m.MatchVersion != null && dseVersion >= m.MatchVersion).OrderByDescending(m => m.MatchVersion).FirstOrDefault();
                    fileMappings.Add(versionedMapper);
                }

                Logger.Instance.InfoFormat("FileMapper<{0}, {1}>",
                                                mapperId = fileMappings.GetHashCode(),
                                                JsonConvert.SerializeObject(fileMappings));

                try
                {
                    if (DisableParallelProcessing || mapperGroup.Key.ParallelProcessingWithinPriorityLevel == FileMapper.ProcessingTaskOptions.None)
                    {
                        Logger.Instance.InfoFormat("FileMapper<{0}, SyncMode>",
                                                    mapperId);

                        foreach (var fileMapper in fileMappings)
                        {
                            filemapperInError = fileMapper;
                            parallelOptions.CancellationToken.ThrowIfCancellationRequested();

                            if (fileMapper.CancellationSource == null && cancellationSource != null) fileMapper.CancellationSource = cancellationSource;

                            var diagnosticFiles = ProcessFile(diagnosticDirectory, fileMapper, dataCenterName, clusterName, dseVersion);

                            diagFilesList.AddRange(diagnosticFiles);

                            System.Threading.Tasks.Task.WaitAll(diagnosticFiles.Select(d => d.Task).ToArray());
                        }
                    }
                    else if (mapperGroup.Key.ParallelProcessingWithinPriorityLevel.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessing))
                    {
                        Logger.Instance.InfoFormat("FileMapper<{0}, ParallelProcessing>",
                                                    mapperId);

                        Parallel.ForEach(fileMappings, parallelOptions, fileMapper =>
                        {
                            try
                            {
                                if (fileMapper.CancellationSource == null && cancellationSource != null) fileMapper.CancellationSource = cancellationSource;

                                var diagnosticFiles = ProcessFile(diagnosticDirectory, fileMapper, dataCenterName, clusterName, dseVersion);

                                diagFilesList.AddRange(diagnosticFiles);
                                parallelOptions.CancellationToken.ThrowIfCancellationRequested();
                            }
                            catch
                            {
                                filemapperInError = fileMapper;
                                throw;
                            }
                        });

                        Logger.Instance.InfoFormat("FileMapper<{0}, ParallelProcessing, Completed>",
                                                    mapperId);
                    }
                    else if (mapperGroup.Key.ParallelProcessingWithinPriorityLevel.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessingWithinPriorityLevel))
                    {
                        Logger.Instance.InfoFormat("FileMapper<{0}, ParallelProcessingWithinPriorityLevel>",
                                                    mapperId);

                        var localDiagFilesList = new List<DiagnosticFile>();
                        Parallel.ForEach(fileMappings, parallelOptions, fileMapper =>
                        {
                            try
                            {
                                if (fileMapper.CancellationSource == null && cancellationSource != null) fileMapper.CancellationSource = cancellationSource;

                                var diagnosticFiles = ProcessFile(diagnosticDirectory, fileMapper, dataCenterName, clusterName, dseVersion);

                                diagFilesList.AddRange(diagnosticFiles);
                                localDiagFilesList.AddRange(diagnosticFiles);
                                parallelOptions.CancellationToken.ThrowIfCancellationRequested();
                            }
                            catch
                            {
                                filemapperInError = fileMapper;
                                throw;
                            }
                        });

                        Logger.Instance.InfoFormat("FileMapper<{0}, ParallelProcessingWithinPriorityLevel, Waiting>",
                                                    mapperId);

                        System.Threading.Tasks.Task.WaitAll(localDiagFilesList.Select(d => d.Task).ToArray());

                        Logger.Instance.InfoFormat("FileMapper<{0}, ParallelProcessingWithinPriorityLevel, Completed>",
                                                    mapperId);
                    }
                    else
                    {
                        Logger.Instance.ErrorFormat("{0}\t{1}\tError with Mapping's Parallel Processing Type. Unknown Value of \"{1}\".",
                                                        mapperGroup.Key.ProcessPriorityLevel, mapperGroup.Key.ParallelProcessingWithinPriorityLevel);
                    }
                }
                catch (TaskCanceledException)
                {
                    Logger.Instance.WarnFormat("{0}\t{1}\tProcessing was canceled for Mapping Level.",
                                                mapperGroup.Key.ProcessPriorityLevel, mapperGroup.Key.ParallelProcessingWithinPriorityLevel);
                }
                catch (AggregateException ae)
                {
                    bool canceled = false;

                    foreach (Exception e in ae.InnerExceptions)
                    {
                        if (e is TaskCanceledException)
                        {
                            if (!canceled)
                            {
                                canceled = true;
                                Logger.Instance.WarnFormat("{0}\t{1}\tProcessing was canceled for Mapping Level.",
                                                            mapperGroup.Key.ProcessPriorityLevel, mapperGroup.Key.ParallelProcessingWithinPriorityLevel);
                            }
                            continue;
                        }
                        Logger.Instance.Error("Exception during FileMapping Processing", e);
                    }

                    if(!canceled)
                    {
                        ExceptionEventArgs.InvokeEvent(filemapperInError,
                                                        ae,
                                                        cancellationSource,
                                                        new object[] {  diagnosticDirectory,
                                                                        dataCenterName,
                                                                        clusterName,
                                                                        dseVersion },
                                                        OnException);
                    }
                }
                catch (System.Exception ex)
                {
                    Logger.Instance.Error("Exception during FileMapping Processing", ex);

                    ExceptionEventArgs.InvokeEvent(filemapperInError,
                                                       ex,
                                                       cancellationSource,
                                                       new object[] {  diagnosticDirectory,
                                                                        dataCenterName,
                                                                        clusterName,
                                                                        dseVersion },
                                                       OnException);
                }
            }

            return diagFilesList.Count == 0
                        ? Common.Patterns.Tasks.CompletionExtensions.CompletedTask<IEnumerable<DiagnosticFile>>(Enumerable.Empty<DiagnosticFile>())
                        : Task<IEnumerable<DiagnosticFile>>.Factory.ContinueWhenAll(diagFilesList.Select(f => f.Task).ToArray(), ignoreItem => diagFilesList);
        }

        public static IEnumerable<DiagnosticFile> ProcessFile(IDirectoryPath diagnosticDirectory,
                                                                            FileMapper fileMappings,
                                                                            string dataCenterName = null,
                                                                            string clusterName = null,
                                                                            Version targetDSEVersion = null)
		{
            var ignoreFilesRegEx = string.IsNullOrEmpty(fileMappings?.IgnoreFilesMatchingRegEx)
                                            ? null
                                            : new Regex(fileMappings.IgnoreFilesMatchingRegEx,
                                                            RegexOptions.Compiled | RegexOptions.IgnoreCase);
			var mergesFiles = fileMappings?.FilePathMerge(diagnosticDirectory);
            var resultingFiles = mergesFiles == null
                                ? Enumerable.Empty<IPath>()
                                : mergesFiles.SelectMany(f =>
                                    {
                                        try
                                        {
                                            return f.HasWildCardPattern()
                                                    ? f.GetWildCardMatches()
                                                    : new List<IPath>() { f };
                                        }
                                        catch (System.IO.DirectoryNotFoundException) { }
                                        catch (System.IO.FileNotFoundException) { }
                                        return Enumerable.Empty<IPath>();
                                    });
            var targetFiles = resultingFiles.Where(f => f is IFilePath
                                                            && f.Exist())
                                            .Cast<IFilePath>()
                                            .OrderByDescending(f => f.GetLastWriteTimeUtc())
                                            .DuplicatesRemoved(f => f) //If there are duplicates, keep the most current...
                                            .Where(f => ignoreFilesRegEx == null ? true : !ignoreFilesRegEx.IsMatch(f.PathResolved));
			var diagnosticInstances = new List<DiagnosticFile>();
			var instanceType = fileMappings.GetFileParsingType();

            if(instanceType == null)
            {
                Logger.Instance.ErrorFormat("<NoNodeId>\t<NoFile>\tFile Mapper File Parsing Class \"{0}\" was not found for Category {1}",
                                            fileMappings.FileParsingClass,
                                            fileMappings.Catagory);
                return Enumerable.Empty<DiagnosticFile>();
            }

            resultingFiles.Complement(targetFiles)
                            .ForEach(i => Logger.Instance.WarnFormat("<NoNodeId>\t{0}\tFile was skipped for processing because it either doesn't exist or is a file folder.",
                                                                        i.PathResolved));

            if(!string.IsNullOrEmpty(fileMappings.DefaultCluster))
            {
                clusterName = fileMappings.DefaultCluster;
            }
            if(!string.IsNullOrEmpty(fileMappings.DefaultDataCenter))
            {
                dataCenterName = fileMappings.DefaultDataCenter;
            }

            INode[] processTheseNodes = null;

            if(fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.AllNodesInCluster))
            {
                processTheseNodes = Cluster.GetNodes(null, clusterName).ToArray();
            }
            else if (fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.AllNodesInDataCenter))
            {
                processTheseNodes = Cluster.GetNodes(dataCenterName, clusterName).ToArray();
            }

            if (processTheseNodes != null && processTheseNodes.Length == 0)
            {
                Logger.Instance.ErrorFormat("<NoNodeId>\t<NoFile>\tNode Processing Option {0} failed to retreive any node for Cluster \"{1}\", Data Center \"{2}\". File Mapper File Parsing Class \"{3}\" for Category {4} will NOT be processed!",
                                                fileMappings.ProcessingTaskOption,
                                                clusterName,
                                                dataCenterName,
                                                fileMappings.FileParsingClass,
                                                fileMappings.Catagory);
                return Enumerable.Empty<DiagnosticFile>();
            }

            CancellationToken? cancellationToken = fileMappings.CancellationSource?.Token;
            DiagnosticFile resultInstance = null;
            bool onlyOnceProcessing = fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.OnlyOnce);

            //targetFiles
            foreach (var targetFile in targetFiles)
			{

                if (cancellationToken.HasValue && cancellationToken.Value.IsCancellationRequested)
                {
                    break;
                }

                if(onlyOnceProcessing && resultInstance != null && resultInstance.Processed)
                {
                    break;
                }

                resultInstance = null;

                if (processTheseNodes == null)
                {
                    if (fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ScanForNode))
                    {
                        var nodeId = DetermineNodeIdentifier(targetFile, fileMappings.NodeIdPos);

                        if (nodeId == null)
                        {
                            Logger.Instance.ErrorFormat("<NoNodeId>\t{0}\tCouldn't detect node identity (IPAdress or host name) for this file path. This file will be skipped.", targetFile.PathResolved);
                        }
                        else
                        {
                            resultInstance = ProcessFile(diagnosticDirectory,
                                                            targetFile,
                                                            instanceType,
                                                            fileMappings.Catagory,
                                                            nodeId,
                                                            null,
                                                            dataCenterName,
                                                            clusterName,
                                                            cancellationToken,
                                                            targetDSEVersion,
                                                            !DisableParallelProcessing && fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessFiles));

                            if (resultInstance != null)
                            {
                                diagnosticInstances.Add(resultInstance);
                            }
                        }
                    }
                    else
                    {
                        resultInstance = ProcessFile(diagnosticDirectory,
                                                        targetFile,
                                                        instanceType,
                                                        fileMappings.Catagory,
                                                        null,
                                                        null,
                                                        dataCenterName,
                                                        clusterName,
                                                        cancellationToken,
                                                        targetDSEVersion,
                                                        !DisableParallelProcessing && fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessFiles));

                        if (resultInstance != null)
                        {
                            diagnosticInstances.Add(resultInstance);
                        }
                    }
                }
                else
                {
                    foreach (var node in processTheseNodes)
                    {
                        resultInstance = ProcessFile(diagnosticDirectory,
                                                        targetFile,
                                                        instanceType,
                                                        fileMappings.Catagory,
                                                        null,
                                                        node,
                                                        dataCenterName,
                                                        clusterName,
                                                        cancellationToken,
                                                        targetDSEVersion,
                                                        !DisableParallelProcessing && fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessFiles));

                        if (resultInstance != null)
                        {
                            diagnosticInstances.Add(resultInstance);
                            if (onlyOnceProcessing && resultInstance.Processed)
                            {
                                break;
                            }
                        }

                        if(cancellationToken.HasValue && cancellationToken.Value.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                }
			}

			return diagnosticInstances;
		}

		public static DiagnosticFile ProcessFile(IDirectoryPath diagnosticDirectory,
													IFilePath processFile,
													System.Type instanceType,
													CatagoryTypes catagory,
													NodeIdentifier nodeId,
                                                    INode useNode = null,
													string dataCenterName = null,
													string clusterName = null,
                                                    CancellationToken? cancellationToken = null,
                                                    Version targetDSEVersion = null,
                                                    bool runAsTask = false)
		{
			var node = useNode == null ? Cluster.TryGetAddNode(nodeId, dataCenterName, clusterName) : useNode;
			var processingFileInstance = (DiagnosticFile) Activator.CreateInstance(instanceType, catagory, diagnosticDirectory, processFile, node, clusterName, dataCenterName, targetDSEVersion);

            if(cancellationToken.HasValue)
            {
                processingFileInstance.CancellationToken = cancellationToken.Value;
            }

            var action = (Action)(() =>
            {
                try
                {
                    Logger.Instance.InfoFormat("{0}\t{1}\tBegin{2}Processing of File", node, processFile.PathResolved, runAsTask ? " (Async) " : " ");

                    processingFileInstance.CancellationToken.ThrowIfCancellationRequested();

                    var nbrItems = processingFileInstance.ProcessFile();

                    processingFileInstance._nbrItemGenerated = unchecked((int)nbrItems);

                    if (!processingFileInstance.Processed && nbrItems > 0) processingFileInstance.Processed = true;

                    if (nbrItems > 0)
                    {
                        Logger.Instance.InfoFormat("{0}\t{1}\tEnd of Processing of File that resulted in {2:###,##0} objects", node, processFile.PathResolved, nbrItems);
                    }
                    else
                    {
                        Logger.Instance.WarnFormat("{0}\t{1}\tEnd of Processing of File that resulted in {2:###,##0} objects", node, processFile.PathResolved, nbrItems);
                    }
                }
                catch(TaskCanceledException)
                {
                    Logger.Instance.WarnFormat("{0}\t{1}\tProcessing was canceled.",
                                                node, processFile.PathResolved);
                    if(processingFileInstance != null)
                    {
                        processingFileInstance.Canceled = true;
                    }
                }
                catch (AggregateException ae)
                {
                    bool otherExceptions = false;
                    bool canceled = false;

                    foreach (Exception e in ae.InnerExceptions)
                    {
                        if (e is TaskCanceledException)
                        {
                            if (!canceled)
                            {
                                Logger.Instance.WarnFormat("{0}\t{1}\tProcessing was canceled.",
                                                            node, processFile.PathResolved);
                                if (processingFileInstance != null)
                                {
                                    processingFileInstance.Canceled = true;
                                }
                                canceled = true;
                            }
                            continue;
                        }
                        otherExceptions = true;
                    }

                    if(!canceled)
                    {
                        ExceptionEventArgs.InvokeEvent(processingFileInstance,
                                                           ae,
                                                           null,
                                                           new object[] { diagnosticDirectory,
                                                                                processFile,
                                                                                instanceType,
                                                                                catagory,
                                                                                nodeId,
                                                                                useNode,
                                                                                dataCenterName,
                                                                                clusterName,
                                                                                cancellationToken,
                                                                                targetDSEVersion,
                                                                                runAsTask },
                                                           OnException);
                    }

                    if(otherExceptions)
                    {
                        if (processingFileInstance == null)
                        {
                            throw;
                        }

                        processingFileInstance.Exception = ae;
                    }
                }
                catch(System.Exception ex)
                {
                    ExceptionEventArgs.InvokeEvent(processingFileInstance,
                                                       ex,
                                                       null,
                                                       new object[] { diagnosticDirectory,
                                                                            processFile,
                                                                            instanceType,
                                                                            catagory,
                                                                            nodeId,
                                                                            useNode,
                                                                            dataCenterName,
                                                                            clusterName,
                                                                            cancellationToken,
                                                                            targetDSEVersion,
                                                                            runAsTask },
                                                       OnException);

                    if (processingFileInstance == null)
                    {
                        throw;
                    }

                    processingFileInstance.Exception = ex;
                }
            });

            if (runAsTask)
            {
                processingFileInstance.Task = Task<DiagnosticFile>.Factory.StartNew(() => { action(); return processingFileInstance; },
                                                                                        processingFileInstance.CancellationToken);
            }
            else
            {
                action();
                processingFileInstance.Task = Common.Patterns.Tasks.CompletionExtensions.CompletedTask(processingFileInstance);
            }

            return processingFileInstance;
		}

		#endregion

		#region abstracts

		public abstract IResult GetResult();

        /// <summary>
        ///
        ///
        /// Processed
        /// </summary>
        /// <returns>
        /// Returns the number of items created/generated. This number sets the NbrItemGenerated property and if greater than zero the Processed property is set to true.
        /// </returns>
        /// <remarks>
        /// The implication of this method should also set the NbrItemsParsed property to the items read/parsed/reviewed.
        /// </remarks>
		public abstract uint ProcessFile();

		#endregion
	}
}

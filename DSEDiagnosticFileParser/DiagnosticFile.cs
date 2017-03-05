﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Common;
using Common.Path;
using System.IO;
using System.Threading;

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
            ZipFile
		}

        public static Dictionary<string, RegExParseString> RegExAssocations = LibrarySettings.DiagnosticFileRegExAssocations;

		protected DiagnosticFile(CatagoryTypes catagory,
									IDirectoryPath diagnosticDirectory,
									IFilePath file,
									INode node,
                                    string defaultClusterName,
                                    string defaultDCName)
		{
			this.Catagory = catagory;
			this.DiagnosticDirectory = diagnosticDirectory;
			this.File = file;
			this.Node = node;
			this.ParsedTimeRange = new DateTimeRange();
			this.ParsedTimeRange.SetMinimal(DateTime.Now);
            this.DefaultClusterName = defaultClusterName;
            this.DefaultDataCenterName = defaultDCName;

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
        public int NbrItemsParsed { get; protected set; }
		public DateTimeRange ParsedTimeRange { get; protected set; }
        /// <summary>
        /// True to indicate the process was sucessfully completed;
        /// </summary>
        public bool Processed { get; protected set; }
        public System.Exception Exception { get; protected set; }
        public int NbrItemGenerated { get; protected set; }
        public RegExParseString RegExParser { get; protected set; }
        public CancellationToken CancellationToken { get; set; }

        public bool CancelPending { get { return !this.Canceled && this.CancellationToken.IsCancellationRequested; } }
        public bool Canceled { get; protected set; }
        public Task<DiagnosticFile> Task { get; protected set; }
        #endregion

        #region Protected members
        protected DiagnosticFile AssocateItem(IEvent item)
		{
			this.Node.AssocateItem(item);
			return this;
		}
		protected DiagnosticFile AssocateItem(IConfiguration item)
		{
			this.Node.AssocateItem(item);
			return this;
		}
		protected DiagnosticFile AssocateItem(IDDL item)
		{
			this.Node.AssocateItem(item);
			return this;
		}
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
            var diagFilesList = new List<DiagnosticFile>();
            var mappers = DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings
                            .OrderByDescending(o => o.ProcessPriorityLevel)
                            .ThenBy(o => DSEDiagnosticFileParser.FileMapper.DetermineParallelOptions(o.ProcessingTaskOption))
                            .GroupBy(k => new { ProcessPriorityLevel = k.ProcessPriorityLevel, ParallelProcessingWithinPriorityLevel = DSEDiagnosticFileParser.FileMapper.DetermineParallelOptions(k.ProcessingTaskOption) });
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

                try
                {
                    if (mapperGroup.Key.ParallelProcessingWithinPriorityLevel.HasFlag(FileMapper.ProcessingTaskOptions.None))
                    {
                        foreach (var fileMapper in fileMappings)
                        {
                            parallelOptions.CancellationToken.ThrowIfCancellationRequested();

                            if (fileMapper.CancellationSource == null && cancellationSource != null) fileMapper.CancellationSource = cancellationSource;

                            var diagnosticFiles = ProcessFile(diagnosticDirectory, fileMapper, dataCenterName, clusterName);

                            diagFilesList.AddRange(diagnosticFiles);

                            System.Threading.Tasks.Task.WaitAll(diagnosticFiles.Select(d => d.Task).ToArray());
                        }
                    }
                    else if (mapperGroup.Key.ParallelProcessingWithinPriorityLevel.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessing))
                    {
                        Parallel.ForEach(fileMappings, parallelOptions, fileMapper =>
                        {
                            if (fileMapper.CancellationSource == null && cancellationSource != null) fileMapper.CancellationSource = cancellationSource;

                            var diagnosticFiles = ProcessFile(diagnosticDirectory, fileMapper, dataCenterName, clusterName);

                            diagFilesList.AddRange(diagnosticFiles);
                            parallelOptions.CancellationToken.ThrowIfCancellationRequested();
                        });
                    }
                    else if (mapperGroup.Key.ParallelProcessingWithinPriorityLevel.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessingWithinPriorityLevel))
                    {
                        var localDiagFilesList = new List<DiagnosticFile>();
                        Parallel.ForEach(fileMappings, parallelOptions, fileMapper =>
                        {
                            if (fileMapper.CancellationSource == null && cancellationSource != null) fileMapper.CancellationSource = cancellationSource;

                            var diagnosticFiles = ProcessFile(diagnosticDirectory, fileMapper, dataCenterName, clusterName);

                            diagFilesList.AddRange(diagnosticFiles);
                            localDiagFilesList.AddRange(diagnosticFiles);
                            parallelOptions.CancellationToken.ThrowIfCancellationRequested();
                        });
                        System.Threading.Tasks.Task.WaitAll(localDiagFilesList.Select(d => d.Task).ToArray());
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
                    foreach (Exception e in ae.InnerExceptions)
                    {
                        if (e is TaskCanceledException)
                        {
                            Logger.Instance.WarnFormat("{0}\t{1}\tProcessing was canceled for Mapping Level.",
                                                        mapperGroup.Key.ProcessPriorityLevel, mapperGroup.Key.ParallelProcessingWithinPriorityLevel);
                            continue;
                        }
                        Logger.Instance.Error("Exception during FileMapping Processing", e);
                    }
                }
                catch (System.Exception ex)
                {
                    Logger.Instance.Error("Exception during FileMapping Processing", ex);
                }
            }

            return diagFilesList.Count == 0
                        ? Common.Patterns.Tasks.CompletionExtensions.CompletedTask<IEnumerable<DiagnosticFile>>(Enumerable.Empty<DiagnosticFile>())
                        : Task<IEnumerable<DiagnosticFile>>.Factory.ContinueWhenAll(diagFilesList.Select(f => f.Task).ToArray(), ignoreItem => diagFilesList);
        }

        public static IEnumerable<DiagnosticFile> ProcessFile(IDirectoryPath diagnosticDirectory,
                                                                            FileMapper fileMappings,
                                                                            string dataCenterName = null,
                                                                            string clusterName = null)
		{
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
                                                                .DuplicatesRemoved(f => f); //If there are duplicates, keep the most current...
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
                                                            fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessFiles));

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
                                                        fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessFiles));

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
                                                        fileMappings.ProcessingTaskOption.HasFlag(FileMapper.ProcessingTaskOptions.ParallelProcessFiles));

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
                                                                bool runAsTask = false)
		{
			var node = useNode == null ? Cluster.TryGetAddNode(nodeId, dataCenterName, clusterName) : useNode;
			var processingFileInstance = (DiagnosticFile) Activator.CreateInstance(instanceType, catagory, diagnosticDirectory, processFile, node, clusterName, dataCenterName);

            if(cancellationToken.HasValue)
            {
                processingFileInstance.CancellationToken = cancellationToken.Value;
            }

            var action = (Action)(() =>
            {
                try
                {
                    Logger.Instance.InfoFormat("{0}\t{1}\tBegin{2}Processing of File", node, processFile.PathResolved, runAsTask ? " (Async) " : " ");

                    if(processingFileInstance.CancelPending || processingFileInstance.Canceled)
                    {
                        throw new TaskCanceledException();
                    }

                    var nbrItems = processingFileInstance.ProcessFile();

                    processingFileInstance.NbrItemGenerated = (int)nbrItems;

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

                    foreach (Exception e in ae.InnerExceptions)
                    {
                        if (e is TaskCanceledException)
                        {
                            Logger.Instance.WarnFormat("{0}\t{1}\tProcessing was canceled.",
                                                            node, processFile.PathResolved);
                            if (processingFileInstance != null)
                            {
                                processingFileInstance.Canceled = true;
                            }
                            continue;
                        }
                        otherExceptions = true;
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
		public abstract uint ProcessFile();

		#endregion
	}
}

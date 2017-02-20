using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Common;
using Common.Path;
using System.IO;

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
			SystemOutputFile
		}

        public static Dictionary<string, RegExParseString> RegExAssocations = LibrarySettings.DiagnosticFileRegExAssocations;

		protected DiagnosticFile(CatagoryTypes catagory,
									IDirectoryPath diagnosticDirectory,
									IFilePath file,
									INode node)
		{
			this.Catagory = catagory;
			this.DiagnosticDirectory = diagnosticDirectory;
			this.File = file;
			this.Node = node;
			this.ParsedTimeRange = new DateTimeRange();
			this.ParsedTimeRange.SetMinimal(DateTime.Now);

            this.RegExParser = RegExAssocations.TryGetValue(this.GetType().Name);
		}

		#region Public Members
		public CatagoryTypes Catagory { get; private set; }
		public IDirectoryPath DiagnosticDirectory { get; private set; }
		public IFilePath File { get; private set; }
		public INode Node { get; private set; }
        public DateTimeRange ParseOnlyInTimeRange { get; protected set; }
        public int NbrItemsParsed { get; protected set; }
		public DateTimeRange ParsedTimeRange { get; protected set; }
        public bool Processed { get; protected set; }
        public System.Exception Exception { get; protected set; }
        public int NbrItemGenerated { get; protected set; }
        public RegExParseString RegExParser { get; protected set; }
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

			if(nodeId == null && nodeIdPos != 0)
			{
				bool scanning = nodeIdPos < 0;
				var directories = filePath.ParentDirectoryPath.PathResolved.Split(Path.DirectorySeparatorChar);

				if (scanning)
				{
					for (int dirLevel = directories.Length - 1; dirLevel >= 0; --dirLevel)
					{
						nodeId = NodeIdentifier.CreateNodeIdentifer(directories[dirLevel], NodeIdentifier.CreateNodeIdentiferParsingOptions.IPAddressScan | NodeIdentifier.CreateNodeIdentiferParsingOptions.NodeNameEmbedded);

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

		public static IEnumerable<DiagnosticFile> ProcessFile(IDirectoryPath diagnosticDirectory,
                                                                            FileMapper fileMappings,
                                                                            string dataCenterName = null,
                                                                            string clusterName = null)
		{
			var mergesFiles = fileMappings?.FilePathMerge(diagnosticDirectory);
            var resultingFiles = mergesFiles == null
                                ? Enumerable.Empty<IPath>()
                                : mergesFiles.SelectMany(f => f.HasWildCardPattern()
                                                                ? f.GetWildCardMatches()
                                                                : new List<IPath>() { f });
            var targetFiles = resultingFiles.Where(f => f is IFilePath && f.Exist()).Cast<IFilePath>();
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

            //targetFiles
            foreach(var targetFile in targetFiles)
			{
                DiagnosticFile resultInstance = null;

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
                            resultInstance = ProcessFile(diagnosticDirectory, targetFile, instanceType, fileMappings.Catagory, nodeId, null, dataCenterName, clusterName);

                            if (resultInstance != null)
                            {
                                diagnosticInstances.Add(resultInstance);
                            }
                        }
                    }
                    else
                    {
                        resultInstance = ProcessFile(diagnosticDirectory, targetFile, instanceType, fileMappings.Catagory, null, null, dataCenterName, clusterName);

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
                        resultInstance = ProcessFile(diagnosticDirectory, targetFile, instanceType, fileMappings.Catagory, null, node, dataCenterName, clusterName);

                        if (resultInstance != null)
                        {
                            diagnosticInstances.Add(resultInstance);
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
																string clusterName = null)
		{
			var node = useNode == null ? Cluster.TryGetAddNode(nodeId, dataCenterName, clusterName) : useNode;
			var processingFileInstance = (DiagnosticFile) Activator.CreateInstance(instanceType, catagory, diagnosticDirectory, processFile, node);

            Logger.Instance.InfoFormat("{0}\t{1}\tBegin Processing of File", nodeId, processFile.PathResolved);

			//var nbrItems = await Task.Factory.StartNew(() => processingFileInstance.ProcessFile());
            var nbrItems = processingFileInstance.ProcessFile();

            Logger.Instance.InfoFormat("{0}\t{1}\tEnd of Processing of File that resulted in {2:###,##0} objects", nodeId, processFile.PathResolved, nbrItems);

            return processingFileInstance;
		}

		#endregion

		#region abstracts

		public abstract IEnumerable<T> GetItems<T>() where T : IParsed;
		public abstract uint ProcessFile();

		#endregion
	}
}

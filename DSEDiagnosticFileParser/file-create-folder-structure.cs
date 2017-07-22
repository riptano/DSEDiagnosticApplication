using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Common;
using Common.Path;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_create_folder_structure : DiagnosticFile
    {
        public sealed class Mapping
        {
            public string FolderStructureType;

            public RegExParseString SourceMatchRegEx;
            /// <summary>
            /// The target string that is used to create the folder structure and copy the source file to.
            /// Named groups from the SourcsMatchRegEx can be used in the TargetReplaceRegEx following the RegEx replace syntax.
            ///
            /// Special Replace Groups:
            /// ${node} -- The node&apos; IP Address detected by file_create_folder_structure class.
            /// ${filename} -- The target&apos;s file name with extension
            /// ${filenamewoextension} -- The target&apos;s file name without extension
            /// ${fileextension} -- The target&apos;s file extension with period.
            ///
            /// </summary>
            public string TargetReplacePathString;

            /// <summary>
            /// If true (default), the original file is only overwritten if the target file is newer.
            /// </summary>
            public bool CopyOnlyIfNewer = true;

            public bool Enabled = true;
        }

        public sealed class Mappings
        {
            public Mappings(params Mapping[] mappings)
            {
                this.Maps = mappings;
            }

            public Mapping[] Maps { get; set; }

            /// <summary>
            ///
            /// </summary>
            /// <param name="targetPath"></param>
            /// <param name="node"></param>
            /// <returns>
            /// Returns null if targetPath has no matches otherwise the source path is returned.
            /// </returns>
            public Tuple<IPath, bool> MatchAndReplace(IPath targetPath, IDirectoryPath targetDirectory, INode node, string clusterName, DateTime timestampUTC)
            {
                string targetPathString;
                bool noClusterName = string.IsNullOrEmpty(clusterName);

                foreach(var targetMap in this.Maps)
                {
                    if (targetMap.Enabled)
                    {
                        if ((targetPathString = targetMap.SourceMatchRegEx.IsMatchAnyAndReplace(targetPath.PathResolved, targetMap.TargetReplacePathString)) != null)
                        {
                            if (node != null)
                            {
                                targetPathString = targetPathString.Replace("${node}", node.Id.Addresses.FirstOrDefault()?.ToString());
                            }
                            if (targetPath.IsFilePath)
                            {
                                targetPathString = targetPathString.Replace("${filenamewoextension}", ((IFilePath)targetPath).FileNameWithoutExtension);
                                targetPathString = targetPathString.Replace("${filename}", ((IFilePath)targetPath).FileName);
                                targetPathString = targetPathString.Replace("${fileextension}", ((IFilePath)targetPath).FileExtension);
                            }

                            targetPathString = targetPathString.Replace("${directoryname}",
                                                                            string.Format(Properties.Settings.Default.FileCreateFolderDefaultDirFormatString,
                                                                                            noClusterName
                                                                                                ? Properties.Settings.Default.FileCreateFolderDefaultDirName
                                                                                                : clusterName,
                                                                                            timestampUTC));

                            return new Tuple<IPath, bool>(PathUtils.Parse(targetPathString,
                                                                            targetDirectory?.Path),
                                                            targetMap.CopyOnlyIfNewer);
                        }
                    }
                }

                return null;
            }
        }

        public file_create_folder_structure(CatagoryTypes catagory,
                                                IDirectoryPath diagnosticDirectory,
                                                IFilePath file,
                                                INode node,
                                                string defaultClusterName,
                                                string defaultDCName,
                                                Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this.TargetSourceMappings = LibrarySettings.FileCreateFolderTargetSourceMappings;
            this.TimeStampUTC = DateTime.UtcNow;
        }

        public DateTime TimeStampUTC { get; }

        [JsonIgnore]
        public Mappings TargetSourceMappings { get; }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class FileResult : IResult
        {
            internal FileResult(IPath copyFile, INode node, bool fileExists)
            {
                this.Path = copyFile;
                this.Node = node;
                this.FileAlreadyExists = fileExists;
            }

            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; }
            public Cluster Cluster { get; }
            public IDataCenter DataCenter { get; }
            public INode Node { get; }
            public int NbrItems { get { return 1; } }

            public bool FileCopiedSuccessfully { get; internal set; }
            public bool FileAlreadyExists { get; }

            /// <summary>
            /// The original (current) file is newer than the file being copied. Original file was not overwritten.
            /// </summary>
            public bool? OriginalVersionNewer { get; internal set; }
            public IEnumerable<IParsed> Results { get; }
        }

        [JsonProperty(PropertyName="Results")]
        private FileResult _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        public override uint ProcessFile()
        {
            if (this.TargetSourceMappings == null) return 0;

            var targetPath = this.TargetSourceMappings.MatchAndReplace(this.File,
                                                                        this.DiagnosticDirectory,
                                                                        this.Node,
                                                                        this.DefaultClusterName,
                                                                        this.TimeStampUTC);

            if (targetPath == null) return 0;
            if (targetPath.Item1.Equals(this.File)) return 0;

            var targetFileInfo = targetPath.Item2 && targetPath.Item1 is IFilePath ? ((IFilePath)targetPath.Item1).FileInfo() : null;

            this._result = new FileResult(targetPath.Item1,
                                            this.Node,
                                            targetFileInfo == null ? targetPath.Item1.Exist() : targetFileInfo.Exists);

            if(targetFileInfo != null
                    && targetFileInfo.LastAccessTimeUtc != DateTime.MinValue
                    && targetFileInfo.Exists)
            {
                var sourceFileInfo = this.File.FileInfo();

                if(targetFileInfo.LastWriteTimeUtc >= sourceFileInfo.LastAccessTimeUtc)
                {
                    this._result.OriginalVersionNewer = true;
                    return 0;
                }
                else
                {
                    this._result.OriginalVersionNewer = false;
                }
            }

            if (this.File.Copy(targetPath.Item1))
            {
                this.NbrItemsParsed = 1;
                this.Processed = true;
                this._result.FileCopiedSuccessfully = true;
            }

            return (uint) 1;
        }
    }
}

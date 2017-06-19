using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Common;
using Common.Path;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
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
        }

        public sealed class Mappings
        {
            public Mappings(params Mapping[] mappings)
            {
                this.Maps = mappings;
            }

            public Mapping[] Maps { get; set; }

            public bool CopyOnlyIfNewer = true;

            /// <summary>
            ///
            /// </summary>
            /// <param name="targetPath"></param>
            /// <param name="node"></param>
            /// <returns>
            /// Returns null if targetPath has no matches otherwise the source path is returned.
            /// </returns>
            public IPath MatchAndReplace(IPath targetPath, IDirectoryPath targetDirectory, INode node)
            {
                string targetPathString;

                foreach(var targetMap in this.Maps)
                {
                    if((targetPathString = targetMap.SourceMatchRegEx.IsMatchAnyAndReplace(targetPath.PathResolved, targetMap.TargetReplacePathString)) != null)
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


                        return PathUtils.Parse(targetPathString,
                                                    targetDirectory?.Path);
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
        }

        public Mappings TargetSourceMappings { get; }

        public sealed class FileResult : IResult
        {
            internal FileResult(IPath copyFile, INode node, bool fileExists)
            {
                this.Path = copyFile;
                this.Node = node;
                this.FileAlreadyExists = fileExists;
            }

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
        private FileResult _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        public override uint ProcessFile()
        {
            if (this.TargetSourceMappings == null) return 0;

            var targetPath = this.TargetSourceMappings.MatchAndReplace(this.File, this.DiagnosticDirectory, this.Node);

            if (targetPath == null) return 0;
            if (targetPath.Equals(this.File)) return 0;

            var targetFileInfo = this.TargetSourceMappings.CopyOnlyIfNewer && targetPath is IFilePath ? ((IFilePath)targetPath).FileInfo() : null;

            this._result = new FileResult(targetPath,
                                            this.Node,
                                            targetFileInfo == null ? targetPath.Exist() : targetFileInfo.Exists);

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

            if (this.File.Copy(targetPath))
            {
                this.NbrItemsParsed = 1;
                this.Processed = true;
                this._result.FileCopiedSuccessfully = true;
            }

            return (uint) 1;
        }
    }
}

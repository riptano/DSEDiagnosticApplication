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

                        return PathUtils.BuildPath(targetPathString,
                                                    targetDirectory?.Path,
                                                    null,
                                                    targetPath.IsFilePath,
                                                    true,
                                                    false,
                                                    false,
                                                    targetPath.IsFilePath
                                                        ? ((IFilePath) targetPath).HasFileExtension
                                                        : false);
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
            internal FileResult(IPath copyFile, INode node)
            {
                this.Path = copyFile;
                this.Node = node;
                this.FileAlreadyExists = copyFile.Exist();
            }

            public IPath Path { get; private set; }
            public Cluster Cluster { get; }
            public IDataCenter DataCenter { get; }
            public INode Node { get; private set; }
            public int NbrItems { get { return 1; } }

            public bool FileCopiedSuccessfully { get; internal set; }
            public bool FileAlreadyExists { get; private set; }

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

            this._result = new FileResult(targetPath, this.Node);

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

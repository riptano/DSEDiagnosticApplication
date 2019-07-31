using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using Common;
using Common.Path;
using Common.Patterns.Threading;
using Microsoft.WindowsAPICodePack.Dialogs;

namespace DSEDiagnosticApplication
{
	public partial class FormMain : Form
	{
        CancellationTokenSource _cancellationSource;
        Common.WaitCursor _waitCusor = new Common.WaitCursor();
        IEnumerable<DSEDiagnosticFileParser.DiagnosticFile> _currentDiagnosticFiles = null;

        public FormMain()
		{
			InitializeComponent();
        }

        #region Form events

        private void FormMain_Load(object sender, EventArgs e)
        {
            if (!string.IsNullOrEmpty(Properties.Settings.Default.DefaultDiagnosticsFolder)
                    && System.IO.Directory.Exists(Properties.Settings.Default.DefaultDiagnosticsFolder))
            {
                this.ultraTextEditorDiagnosticsFolder.Text = Properties.Settings.Default.DefaultDiagnosticsFolder;
            }
            else
            {
                this.ultraTextEditorDiagnosticsFolder.Text = @"C:\";
            }
            this.ultraTextEditorProcessMapperJSONFile.Text = null;
        }

        #endregion

        #region Button Click Events

        private async void buttonAsyncExec_Click(object sender, EventArgs e)
        {
            var buttonLabel = this.buttonAsyncExcute.Text;
            this.buttonAsyncExcute.Enabled = false;
            this.buttonAsyncExcute.Text = "Running...";
            this.buttonSyncExcute.Enabled = false;
            this.buttonSyncExcute.Visible = false;
            this._cancellationSource = new CancellationTokenSource();

            this.buttonCancelExecution.Enabled = true;
            this.buttonCancelExecution.Text = "Cancel";

            this.ultraTextEditorProcessMapperJSONFile.Enabled = false;
            this.ultraTextEditorDiagnosticsFolder.Enabled = false;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = false;
            this.ultraCheckEditorDisableSysDSEDDL.Enabled = false;
            this.ultraCheckEditorLogDisableMryMap.Enabled = false;
            this.ultraTextEditorCluster.Enabled = false;
            this.ultraTextEditorDC.Enabled = false;
            this.ultraNumericEditorClusterHashcode.Enabled = false;
            this.ultraTextEditorAdditionalDDLFiles.Enabled = false;
            this.ultraTextEditorAdditionalLogs.Enabled = false;
            this.ultraTextEditorAltCompFiles.Enabled = false;
            this.ultraDateTimeEditorEndLog.Enabled = false;
            this.ultraDateTimeEditorStartLog.Enabled = false;
            this.ultraTextEditorAdditionalDDLFiles.Enabled = false;
            this.ultraTextEditorAdditionalLogs.Enabled = false;
            this.ultraTextEditorAltCompFiles.Enabled = false;
            this.ultraDateTimeEditorEndLog.Enabled = false;
            this.ultraDateTimeEditorStartLog.Enabled = false;
            this.buttonGenJSON.Enabled = false;

            var items = await this.RunProcessFile().ConfigureAwait(false);

            this.ultraGrid1.DataSource = this._currentDiagnosticFiles = items;

            this.EnableRunButton(buttonLabel);
        }

        private void buttonSyncExcute_Click(object sender, EventArgs e)
        {
            var buttonLabel = this.buttonAsyncExcute.Text;
            this.buttonAsyncExcute.Enabled = false;
            this.buttonAsyncExcute.Text = "Sync-Running...";
            this.buttonSyncExcute.Enabled = false;
            this.buttonSyncExcute.Visible = false;
            this.buttonOpenLogFile.Enabled = false;
            this.buttonOpenLogFile.Visible = false;
            //this._cancellationSource = new CancellationTokenSource();

            //this.button2.Enabled = true;
            //this.button2.Text = "Cancel";

            this.ultraTextEditorProcessMapperJSONFile.Enabled = false;
            this.ultraTextEditorDiagnosticsFolder.Enabled = false;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = false;
            this.ultraCheckEditorDisableSysDSEDDL.Enabled = false;
            this.ultraCheckEditorLogDisableMryMap.Enabled = false;
            this.ultraTextEditorCluster.Enabled = false;
            this.ultraNumericEditorClusterHashcode.Enabled = false;
            this.ultraTextEditorDC.Enabled = false;
            this.ultraTextEditorAdditionalDDLFiles.Enabled = false;
            this.ultraTextEditorAdditionalLogs.Enabled = false;
            this.ultraTextEditorAltCompFiles.Enabled = false;
            this.ultraDateTimeEditorEndLog.Enabled = false;
            this.ultraDateTimeEditorStartLog.Enabled = false;
            this.ultraTextEditorAdditionalDDLFiles.Enabled = false;
            this.ultraTextEditorAdditionalLogs.Enabled = false;
            this.ultraTextEditorAltCompFiles.Enabled = false;
            this.ultraDateTimeEditorEndLog.Enabled = false;
            this.ultraDateTimeEditorStartLog.Enabled = false;
            this.buttonGenJSON.Enabled = false;

            var items = this.RunSyncProcessFile();

            this.ultraGrid1.DataSource = this._currentDiagnosticFiles = items;

            this.EnableRunButton(buttonLabel);
        }

        private void buttonCancelExcution_Click(object sender, EventArgs e)
        {
            if (this._cancellationSource != null)
            {
                this.buttonCancelExecution.Enabled = false;
                this._cancellationSource.Cancel();
                this.buttonCancelExecution.Text = "Canceling";
            }
        }

        private void ultraTextEditorDiagnosticsFolder_EditorButtonClick(object sender, Infragistics.Win.UltraWinEditors.EditorButtonEventArgs e)
        {
            if (e.Button.Key == "DirEditor")
            {
                CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                dialog.InitialDirectory = this.ultraTextEditorDiagnosticsFolder.Text ?? @"C:\";
                dialog.IsFolderPicker = true;
                dialog.Title = "Choose Diagnostics Folder Location";
                dialog.EnsurePathExists = true;
                if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
                {
                    this.ultraTextEditorDiagnosticsFolder.Text = dialog.FileName;
                }
            }
        }

        private void ultraTextEditorProcessMapperJSONFile_EditorButtonClick(object sender, Infragistics.Win.UltraWinEditors.EditorButtonEventArgs e)
        {
            if (e.Button.Key == "FileEditor")
            {
                CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                if (string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
                {
                    if (string.IsNullOrEmpty(Properties.Settings.Default.DefaultProcessMapperJSONFile)
                        || !System.IO.File.Exists(Properties.Settings.Default.DefaultProcessMapperJSONFile))
                    {
                        dialog.InitialDirectory = this.ultraTextEditorDiagnosticsFolder.Text ?? @"C:\";
                        dialog.DefaultExtension = "json";
                    }
                    else
                    {
                        dialog.InitialDirectory = System.IO.Path.GetDirectoryName(Properties.Settings.Default.DefaultProcessMapperJSONFile).ToString();
                        dialog.DefaultFileName = System.IO.Path.GetFileName(Properties.Settings.Default.DefaultProcessMapperJSONFile);
                        dialog.DefaultExtension = System.IO.Path.GetExtension(Properties.Settings.Default.DefaultProcessMapperJSONFile);
                    }
                }
                else
                {
                    dialog.InitialDirectory = System.IO.Path.GetDirectoryName(this.ultraTextEditorProcessMapperJSONFile.Text).ToString();
                    dialog.DefaultFileName = System.IO.Path.GetFileName(this.ultraTextEditorProcessMapperJSONFile.Text);
                    dialog.DefaultExtension = System.IO.Path.GetExtension(this.ultraTextEditorProcessMapperJSONFile.Text);
                }

                dialog.IsFolderPicker = false;
                dialog.Filters.Add(new CommonFileDialogFilter("JSON", "*.json"));
                dialog.Title = "Choose Process Mapper JSON file";
                dialog.EnsureFileExists = true;
                if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
                {
                    this.ultraTextEditorProcessMapperJSONFile.Text = dialog.FileName;
                }
            }
            else if (e.Button.Key == "StringEditor")
            {
                var dialog = new FormJSONEditor();

                if (!string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text)
                        && this.ultraTextEditorProcessMapperJSONFile.Text.Length > 2
                        && (this.ultraTextEditorProcessMapperJSONFile.Text[1] == ':'
                                || this.ultraTextEditorProcessMapperJSONFile.Text[0] == '.'
                                || this.ultraTextEditorProcessMapperJSONFile.Text[0] == '\\'))
                {
                    dialog.Text = System.IO.File.ReadAllText(this.ultraTextEditorProcessMapperJSONFile.Text);
                }
                else
                {
                    dialog.Text = this.ultraTextEditorProcessMapperJSONFile.Text;
                }

                if (dialog.ShowDialog(this) == DialogResult.OK)
                {
                    this.ultraTextEditorProcessMapperJSONFile.Text = dialog.Text;
                }
            }
        }

        private void buttonClearResults_Click(object sender, EventArgs e)
        {
            using (var waitCusor = Common.WaitCursor.UsingCreate(this.ultraGrid1, Common.Patterns.WaitCursorModes.GUI))
            {
                DSEDiagnosticLibrary.Cluster.Clear();
                this.ultraGrid1.DataSource = this._currentDiagnosticFiles = null;
                this._progressionMsgs.Clear();
                this._currentOperations = 0;
                this.SetLoggingStatus("Cleared");
            }
        }

        private void buttonOpenLogFile_Click(object sender, EventArgs e)
        {
            var textPad = new System.Diagnostics.Process();
            textPad.StartInfo.FileName = "textpad.exe";
            textPad.StartInfo.Arguments = @".\DSEDiagnosticApplication.log";
            textPad.Start();
        }

        private void ultraTextEditorAdditionalLogs_EditorButtonClick(object sender, Infragistics.Win.UltraWinEditors.EditorButtonEventArgs e)
        {
            if (e.Button.Key == "FileEditor")
            {
                CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                dialog.InitialDirectory = this.ultraTextEditorDiagnosticsFolder.Text ?? @"C:\";
                dialog.IsFolderPicker = false;
                dialog.Title = "Choose Alternative Log Files";
                dialog.EnsureFileExists = true;
                dialog.Filters.Add(new CommonFileDialogFilter("Log Files", ".log"));
                dialog.Filters.Add(new CommonFileDialogFilter("All Files", ".*"));
                dialog.Multiselect = true;
                dialog.DefaultExtension = ".log";
                if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
                {
                    this.ultraTextEditorAdditionalLogs.Text = string.Join(System.Environment.NewLine, dialog.FileNames);
                }
            }
        }

        private void ultraTextEditorAdditionalDDLFiles_EditorButtonClick(object sender, Infragistics.Win.UltraWinEditors.EditorButtonEventArgs e)
        {
            if (e.Button.Key == "FileEditor")
            {
                CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                dialog.InitialDirectory = this.ultraTextEditorDiagnosticsFolder.Text ?? @"C:\";
                dialog.IsFolderPicker = false;
                dialog.Title = "Choose Alternative DDL Files";
                dialog.EnsureFileExists = true;
                dialog.Filters.Add(new CommonFileDialogFilter("CQL Files", ".cql,.ddl"));
                dialog.Filters.Add(new CommonFileDialogFilter("All Files", ".*"));
                dialog.Multiselect = true;

                if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
                {
                    this.ultraTextEditorAdditionalDDLFiles.Text = string.Join(System.Environment.NewLine, dialog.FileNames);
                }
            }
        }

        private void ultraTextEditorAltCompFiles_EditorButtonClick(object sender, Infragistics.Win.UltraWinEditors.EditorButtonEventArgs e)
        {
            if (e.Button.Key == "FileEditor")
            {
                CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                dialog.InitialDirectory = this.ultraTextEditorDiagnosticsFolder.Text ?? @"C:\";
                dialog.IsFolderPicker = false;
                dialog.Title = "Choose Alternative Compression Files";
                dialog.EnsureFileExists = true;
                dialog.Multiselect = true;
                dialog.DefaultExtension = ".tar.gz";
                dialog.Filters.Add(new CommonFileDialogFilter("Compression Files", ".zip,.tar,.tar.gz"));
                dialog.Filters.Add(new CommonFileDialogFilter("All Files", ".*"));

                if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
                {
                    this.ultraTextEditorAltCompFiles.Text = string.Join(System.Environment.NewLine, dialog.FileNames);
                }
            }
        }

        private void buttonGenJSON_Click(object sender, EventArgs e)
        {
            var diagPath = PathUtils.BuildDirectoryPath(this.ultraTextEditorDiagnosticsFolder.Text);
            var nbrCluster = DSEDiagnosticLibrary.Cluster.Clusters.Count;

            foreach (var cluster in DSEDiagnosticLibrary.Cluster.Clusters)
            {
                if (nbrCluster > 1 && cluster.IsMaster) continue;

                IFilePath jsonFile;

                if(diagPath.MakeFile(cluster.Name, "json", out jsonFile))
                {
                    var json = Newtonsoft.Json.JsonConvert.SerializeObject(cluster,
                                                                            Newtonsoft.Json.Formatting.Indented,
                                                                            new Newtonsoft.Json.JsonSerializerSettings
                                                                            {
                                                                                ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore,
                                                                                PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects,
                                                                                Formatting = Newtonsoft.Json.Formatting.Indented

                                                                            });

                    if(!string.IsNullOrEmpty(json))
                    {
                        jsonFile.WriteAllText(json);
                    }
                }
            }
        }

        #endregion

        #region Execution/Operation Methods
        delegate void EnableRunButtondEL(string buttonLabel);

        void EnableRunButton(string buttonLabel)
        {
            if(this.buttonAsyncExcute.InvokeRequired)
            {
                this.Invoke(new EnableRunButtondEL(this.EnableRunButton), buttonLabel);
                return;
            }

            this.buttonAsyncExcute.Text = buttonLabel;
            this.buttonAsyncExcute.Enabled = true;

            this.buttonSyncExcute.Enabled = true;
            this.buttonSyncExcute.Visible = true;

            this.buttonCancelExecution.Enabled = false;
            this.buttonCancelExecution.Text = this._cancellationSource == null
                                    ? "Cancel"
                                    : (this._cancellationSource.IsCancellationRequested ? "Canceled" : "Cancel");

            this.buttonOpenLogFile.Enabled = true;
            this.buttonOpenLogFile.Visible = true;

            this.ultraTextEditorProcessMapperJSONFile.Enabled = true;
            this.ultraTextEditorDiagnosticsFolder.Enabled = true;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = true;
            this.ultraCheckEditorDisableSysDSEDDL.Enabled = true;
            this.ultraCheckEditorLogDisableMryMap.Enabled = true;
            this.ultraNumericEditorClusterHashcode.Enabled = true;
            this.ultraTextEditorCluster.Enabled = true;
            this.ultraTextEditorDC.Enabled = true;
            this.ultraTextEditorAdditionalDDLFiles.Enabled = true;
            this.ultraTextEditorAdditionalLogs.Enabled = true;
            this.ultraTextEditorAltCompFiles.Enabled = true;
            this.ultraDateTimeEditorEndLog.Enabled = true;
            this.ultraDateTimeEditorStartLog.Enabled = true;
            this.buttonGenJSON.Enabled = true;
        }

        delegate void SetLabelDelegate(string newvalue);

        void SetLoggingStatus(string newValue)
        {
            if(this.ultraStatusBar1.InvokeRequired)
            {
                this.Invoke(new SetLabelDelegate(this.SetLoggingStatus), newValue);
                return;
            }

            this.ultraStatusBar1.Panels["LoggingStatus"].Text = newValue;
            this.ultraStatusBar1.Panels["Operations"].Text = this._currentOperations.ToString("#,###,##0");
        }

        private string _lastLogMsg = string.Empty;
        private void Instance_OnLoggingEventArgs(DSEDiagnosticLogger.Logger sender, DSEDiagnosticLogger.LoggingEventArgs eventArgs)
        {
            foreach (var logMsg in eventArgs.RenderedLogMessages())
            {
                this._lastLogMsg = logMsg;
            }
        }

        private IEnumerable<KeyValuePair<string, IFilePath>> ProcessAdditionalFiles()
        {
            var additionalFiles = new List<KeyValuePair<string, IFilePath>>();

            if(!string.IsNullOrEmpty(this.ultraTextEditorAltCompFiles.Text))
            {
                additionalFiles.AddRange(this.ultraTextEditorAltCompFiles.Text
                                            .Split(new string[] { System.Environment.NewLine, "," }, StringSplitOptions.RemoveEmptyEntries)
                                            .Select(p => new KeyValuePair<string, IFilePath>("ZipFile", PathUtils.BuildFilePath(p))));
            }

            if (!string.IsNullOrEmpty(this.ultraTextEditorAdditionalLogs.Text))
            {
                additionalFiles.AddRange(this.ultraTextEditorAdditionalLogs.Text
                                            .Split(new string[] { System.Environment.NewLine, "," }, StringSplitOptions.RemoveEmptyEntries)
                                            .Select(p => new KeyValuePair<string, IFilePath>("LogFile", PathUtils.BuildFilePath(p))));
            }

            if (!string.IsNullOrEmpty(this.ultraTextEditorAdditionalDDLFiles.Text))
            {
                additionalFiles.AddRange(this.ultraTextEditorAdditionalDDLFiles.Text
                                            .Split(new string[] { System.Environment.NewLine, "," }, StringSplitOptions.RemoveEmptyEntries)
                                            .Select(p => new KeyValuePair<string, IFilePath>("CQLFile", PathUtils.BuildFilePath(p))));
            }

            return additionalFiles.Count == 0 ? null : additionalFiles;
        }


        private async Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> RunProcessFile()
        {
            using (var waitCusor = Common.WaitCursor.UsingCreate(this.ultraGrid1, Common.Patterns.WaitCursorModes.GUI))
            {
                Logger.Instance.Info("test");
                Logger.Instance.OnLoggingEvent += Instance_OnLoggingEventArgs;
                DSEDiagnosticFileParser.DiagnosticFile.OnException += DiagnosticFile_OnException;
                DSEDiagnosticFileParser.DiagnosticFile.OnProgression += DiagnosticFile_OnProgression;
                DSEDiagnosticFileParser.DiagnosticFile.DisableParallelProcessing = this.ultraCheckEditorDisableParallelProcessing.Checked;
                DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = !this.ultraCheckEditorLogDisableMryMap.Checked;

                if(this.ultraCheckEditorDisableSysDSEDDL.Checked)
                {
                    DSEDiagnosticFileParser.cql_ddl.DisabledSystemDSEDefaultKeyspacesInitialLoading();
                }

                if (this.ultraDateTimeEditorStartLog.Value == null
                        || this.ultraDateTimeEditorEndLog.Value == null
                        || (DateTime) this.ultraDateTimeEditorStartLog.Value >= (DateTime) this.ultraDateTimeEditorEndLog.Value)
                {
                    DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange = null;
                }
                else
                {
                    var logStart = (DateTime)this.ultraDateTimeEditorStartLog.Value;
                    var logEnd = (DateTime)this.ultraDateTimeEditorEndLog.Value;
                    var utcStart = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorStartLog.ButtonsRight[0]).Checked;
                    var utcEnd = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorEndLog.ButtonsRight[0]).Checked;

                    DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange = new DateTimeOffsetRange(utcStart
                                                                                                        ? new DateTimeOffset(logStart, TimeSpan.Zero)
                                                                                                        : logStart,
                                                                                                    utcEnd
                                                                                                        ? new DateTimeOffset(logEnd, TimeSpan.Zero)
                                                                                                        : logEnd);
                }

                if (!string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
                {
                    DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings = DSEDiagnosticParamsSettings.Helpers.ReadJsonFileIntoObject<DSEDiagnosticFileParser.FileMapper[]>(this.ultraTextEditorProcessMapperJSONFile.Text);
                }

                var diagPath = PathUtils.BuildDirectoryPath(this.ultraTextEditorDiagnosticsFolder.Text);

                var task = await DSEDiagnosticFileParser.DiagnosticFile.ProcessFileWaitable(diagPath,
                                                                                            this.ultraTextEditorDC.Text,
                                                                                            this.ultraTextEditorCluster.Text,
                                                                                            (int?)this.ultraNumericEditorClusterHashcode.Value,
                                                                                            null,
                                                                                            this._cancellationSource,
                                                                                            this.ProcessAdditionalFiles());

                if (task.Any(t => t.Processed))
                {
                    this.SetLoggingStatus("all processed");
                }
                else
                {
                    this.SetLoggingStatus("NOT all processed");
                }

                return task;
            }
        }

        private IEnumerable<DSEDiagnosticFileParser.DiagnosticFile> RunSyncProcessFile()
        {
            using (var waitCusor = Common.WaitCursor.UsingCreate(this.ultraGrid1, Common.Patterns.WaitCursorModes.GUI))
            {
                Logger.Instance.Info("sync-test");
                Logger.Instance.OnLoggingEvent += Instance_OnLoggingEventArgs;
                DSEDiagnosticFileParser.DiagnosticFile.OnException += DiagnosticFile_OnException;
                DSEDiagnosticFileParser.DiagnosticFile.OnProgression += DiagnosticFile_OnProgression;
                DSEDiagnosticFileParser.DiagnosticFile.DisableParallelProcessing = this.ultraCheckEditorDisableParallelProcessing.Checked;
                DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = !this.ultraCheckEditorLogDisableMryMap.Checked;

                if (this.ultraDateTimeEditorStartLog.Value == null
                        || this.ultraDateTimeEditorEndLog.Value == null
                        || (DateTime)this.ultraDateTimeEditorStartLog.Value >= (DateTime)this.ultraDateTimeEditorEndLog.Value)
                {
                    DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange = null;
                }
                else
                {
                    var logStart = (DateTime) this.ultraDateTimeEditorStartLog.Value;
                    var logEnd = (DateTime)this.ultraDateTimeEditorEndLog.Value;
                    var utcStart = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorStartLog.ButtonsRight[0]).Checked;
                    var utcEnd = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorEndLog.ButtonsRight[0]).Checked;

                    DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange = new DateTimeOffsetRange(utcStart
                                                                                                        ? new DateTimeOffset(logStart, TimeSpan.Zero)
                                                                                                        : logStart,
                                                                                                    utcEnd
                                                                                                        ? new DateTimeOffset(logEnd, TimeSpan.Zero)
                                                                                                        : logEnd);
                }

                if (!string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
                {
                    DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings = DSEDiagnosticParamsSettings.Helpers.ReadJsonFileIntoObject<DSEDiagnosticFileParser.FileMapper[]>(this.ultraTextEditorProcessMapperJSONFile.Text);
                }

                var diagPath = PathUtils.BuildDirectoryPath(this.ultraTextEditorDiagnosticsFolder.Text);

                var task = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(diagPath,
                                                                                this.ultraTextEditorDC.Text,
                                                                                this.ultraTextEditorCluster.Text,
                                                                                (int?) this.ultraNumericEditorClusterHashcode.Value,
                                                                                null,
                                                                                null,
                                                                                this.ProcessAdditionalFiles()).Result;

                //task.Wait();

                //var task = await DSEDiagnosticFileParser.DiagnosticFile.ProcessFileWaitable(diagPath, this.ultraTextEditorDC.Text, this.ultraTextEditorCluster.Text, null, this._cancellationSource);

                if (task.Any(t => t.Processed))
                {
                    this.SetLoggingStatus("all processed");
                }
                else
                {
                    this.SetLoggingStatus("NOT all processed");
                }

                return task;
            }
        }


        private Common.Patterns.Collections.ThreadSafe.List<Tuple<int, string>> _progressionMsgs = new Common.Patterns.Collections.ThreadSafe.List<Tuple<int, string>>();
        private long _currentOperations = 0;

        private void DiagnosticFile_OnProgression(object sender, DSEDiagnosticFileParser.ProgressionEventArgs eventArgs)
        {
            if ((eventArgs.Category & DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Start) != 0)
            {
                var key = (eventArgs.StepName + eventArgs.ThreadId.ToString()).GetHashCode();

                if (!this._progressionMsgs.Any(i => i.Item1 == key))
                {
                    var msg = string.Format("{0}: {1} {2}",
                                                eventArgs.StepName,
                                                sender is DSEDiagnosticFileParser.FileMapper
                                                    ? ((DSEDiagnosticFileParser.FileMapper) sender).Catagory.ToString()
                                                    : (sender is DSEDiagnosticFileParser.DiagnosticFile
                                                            ? ((DSEDiagnosticFileParser.DiagnosticFile)sender).Catagory.ToString()
                                                            : string.Empty),
                                                eventArgs.Message());
                    this._progressionMsgs.Add(new Tuple<int, string>(key, msg));

                    if(eventArgs.Step.HasValue && eventArgs.NbrSteps.HasValue && eventArgs.Step.Value == 1)
                        System.Threading.Interlocked.Add(ref this._currentOperations, (long) eventArgs.NbrSteps.Value);
                }
            }
            else if ((eventArgs.Category & DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End) != 0 || (eventArgs.Category & DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel) != 0)
            {
                var key = (eventArgs.StepName + eventArgs.ThreadId.ToString()).GetHashCode();

                if(this._progressionMsgs.RemoveAll(i => i.Item1 == key) > 0)
                {
                    if (eventArgs.Step.HasValue && eventArgs.NbrSteps.HasValue && eventArgs.Step.Value == eventArgs.NbrSteps.Value)
                        System.Threading.Interlocked.Add(ref this._currentOperations, ((long)eventArgs.NbrSteps.Value) * -1);
                }               
            }
        }

        private void DiagnosticFile_OnException(object sender, DSEDiagnosticFileParser.ExceptionEventArgs eventArgs)
        {
            this._progressionMsgs.Add(new Tuple<int, string>(eventArgs.Exception.GetHashCode(), string.Format("Exception: {0}", eventArgs.Exception.Message)));
        }

         

        #endregion

        #region Grid Events
        private void ultraGrid1_InitializeLayout(object sender, Infragistics.Win.UltraWinGrid.InitializeLayoutEventArgs e)
        {
            var resultCol = e.Layout.Bands[0].Columns["Result"];

            resultCol.CellActivation = Infragistics.Win.UltraWinGrid.Activation.ActivateOnly;
            resultCol.CellAppearance.ForeColor = Color.Blue;
            resultCol.CellAppearance.FontData.Underline = Infragistics.Win.DefaultableBoolean.True;
            resultCol.CellAppearance.Cursor = Cursors.Hand;

            resultCol = e.Layout.Rows.Band.Columns["ExportResults"];
            resultCol.CellAppearance.ForeColor = Color.Blue;
            resultCol.CellAppearance.FontData.Underline = Infragistics.Win.DefaultableBoolean.True;
            resultCol.CellAppearance.Cursor = Cursors.Arrow;
            resultCol.Header.VisiblePosition = e.Layout.Bands[0].Columns.Count - 1;

            resultCol = e.Layout.Rows.Band.Columns["ShowLogEventDialog"];
            resultCol.CellAppearance.ForeColor = Color.Blue;
            resultCol.CellAppearance.FontData.Underline = Infragistics.Win.DefaultableBoolean.True;
            resultCol.CellAppearance.Cursor = Cursors.Arrow;
            resultCol.Header.VisiblePosition = e.Layout.Bands[0].Columns.Count - 1;
        }

        private void ultraGrid1_InitializeRow(object sender, Infragistics.Win.UltraWinGrid.InitializeRowEventArgs e)
        {
            if(!e.ReInitialize)
            {
                var resultValue = e.Row.Cells["Result"].Value;

                if(resultValue is DSEDiagnosticLibrary.IResult)
                {
                    var nbrItems = ((DSEDiagnosticLibrary.IResult)resultValue).NbrItems;

                    if (nbrItems > 0)
                    {
                        e.Row.Cells["ExportResults"].Value = string.Format("{0:###,###,##0} ...", ((DSEDiagnosticLibrary.IResult)resultValue).NbrItems);
                        e.Row.Cells["ExportResults"].ToolTipText = "Click to Export to Excel";
                    }
                    else
                    {
                        e.Row.Cells["ExportResults"].Value = null;
                        e.Row.Cells["ExportResults"].Hidden = true;
                    }
                }
                else
                {
                    e.Row.Cells["ExportResults"].Value = null;
                    e.Row.Cells["ExportResults"].Hidden = true;
                }

                if (resultValue is DSEDiagnosticFileParser.file_cassandra_log4net.LogResults)
                {
                    var nbrItems = ((DSEDiagnosticLibrary.IResult)resultValue).NbrItems;

                    if (nbrItems > 0)
                    {
                        e.Row.Cells["ShowLogEventDialog"].Value = "Show Log Events";
                        e.Row.Cells["ShowLogEventDialog"].ToolTipText = "Click to display Log Event Dialog";
                    }
                    else
                    {
                        e.Row.Cells["ShowLogEventDialog"].Value = null;
                        e.Row.Cells["ShowLogEventDialog"].Hidden = true;
                    }
                }
                else
                {
                    e.Row.Cells["ShowLogEventDialog"].Value = null;
                    e.Row.Cells["ShowLogEventDialog"].Hidden = true;
                }
            }
        }

        private void ultraGrid1_MouseDown(object sender, System.Windows.Forms.MouseEventArgs e)
        {
            var objClickedElement = this.ultraGrid1.DisplayLayout.UIElement.ElementFromPoint(this.ultraGrid1.PointToClient(FormMain.MousePosition));

            if (objClickedElement == null) return;

            var objCellElement = (Infragistics.Win.UltraWinGrid.CellUIElement)objClickedElement.GetAncestor(typeof(Infragistics.Win.UltraWinGrid.CellUIElement));

            if (objCellElement == null) return;

            var objCell = (Infragistics.Win.UltraWinGrid.UltraGridCell)objCellElement.GetContext(typeof(Infragistics.Win.UltraWinGrid.UltraGridCell));

            if (objCell == null) return;

            if (objCell.Column.Key == "Result")
            {
                var gridDialog = new FormObjectGrid();
                var propertyTbl = new PropertyTable();
                string dialogText = null;
                object itemValue;

                //var resultArray = Array.CreateInstance(objCell.Value.GetType(), 2);

                //resultArray.SetValue(objCell.Value, 0);
                //resultArray.SetValue(((DSEDiagnosticLibrary.IResult)objCell.Value).Cluster, 1);
                //
                using (var waitCusor = Common.WaitCursor.UsingCreate(this.ultraGrid1, Common.Patterns.WaitCursorModes.GUI))
                {
                    propertyTbl.Properties.Add(new PropertySpec("Result", objCell.Value.GetType(), "Result"));
                    propertyTbl["Result"] = objCell.Value;

                    if (objCell.Value != null)
                    {
                        {
                            var resultInstance = objCell.Value as DSEDiagnosticLibrary.IResult;

                            if(resultInstance != null)
                            {
                                dialogText = string.Format("{0} {1}",
                                                            resultInstance.Node?.Id.NodeName(),
                                                            resultInstance.Path?.Name);
                            }
                        }
                        bool isLog4NetResult = objCell.Value is DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;
                        foreach (var prop in objCell.Value.GetType().GetFields())
                        {
                            var propType = prop.FieldType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.FieldType)
                                            ? typeof(string[])
                                            : typeof(string);

                            propertyTbl.Properties.Add(new PropertySpec(prop.Name, propType, "Result"));
                            itemValue = prop.GetValue(objCell.Value);

                            if (propType.IsArray)
                            {
                                propertyTbl[prop.Name] = ((IEnumerable<object>)itemValue).Select(i => i.ToString()).ToArray();
                            }
                            else
                            {
                                propertyTbl[prop.Name] = itemValue?.ToString();
                            }
                        }
                        foreach (var prop in objCell.Value.GetType().GetProperties())
                        {
                            itemValue = prop.GetValue(objCell.Value);

                            if (isLog4NetResult && prop.Name == "Results" && itemValue != null)
                            {
                                propertyTbl.Properties.Add(new PropertySpec(prop.Name, typeof(string[]), "Result"));
                                propertyTbl[prop.Name] = ((DSEDiagnosticFileParser.file_cassandra_log4net.LogResults)objCell.Value).ResultsTask.Result
                                                            .Select(i => i.ToString()).ToArray();
                            }
                            else
                            {
                                var propType = prop.PropertyType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType)
                                            ? typeof(string[])
                                            : typeof(string);

                                propertyTbl.Properties.Add(new PropertySpec(prop.Name, propType, "Result"));

                                if (propType.IsArray)
                                {
                                    propertyTbl[prop.Name] = ((IEnumerable<object>)itemValue).Select(i => i.ToString()).ToArray();
                                }
                                else
                                {
                                    propertyTbl[prop.Name] = itemValue?.ToString();
                                }
                            }
                        }
                    }

                    var cluster = ((DSEDiagnosticLibrary.IResult)objCell.Value).Cluster;
                    propertyTbl.Properties.Add(new PropertySpec("Cluster", typeof(DSEDiagnosticLibrary.Cluster), "Custer"));
                    propertyTbl["Cluster"] = cluster;

                    if (cluster != null)
                    {
                        foreach (var prop in cluster.GetType().GetFields())
                        {
                            var propType = prop.FieldType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.FieldType)
                                            ? typeof(string[])
                                            : typeof(string);

                            propertyTbl.Properties.Add(new PropertySpec(prop.Name, propType, "Custer"));
                            itemValue = prop.GetValue(cluster);

                            if (propType.IsArray)
                            {
                                propertyTbl[prop.Name] = ((IEnumerable<object>)itemValue).Select(i => i.ToString()).ToArray();
                            }
                            else
                            {
                                propertyTbl[prop.Name] = itemValue?.ToString();
                            }
                        }
                        foreach (var prop in cluster.GetType().GetProperties())
                        {
                            var propType = prop.PropertyType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType)
                                            ? typeof(string[])
                                            : typeof(string);

                            propertyTbl.Properties.Add(new PropertySpec(prop.Name, propType, "Custer"));
                            itemValue = prop.GetValue(cluster);

                            if (propType.IsArray)
                            {
                                propertyTbl[prop.Name] = ((IEnumerable<object>)itemValue).Select(i => i.ToString()).ToArray();
                            }
                            else
                            {
                                propertyTbl[prop.Name] = itemValue?.ToString();
                            }
                        }
                    }

                    gridDialog.DataSource = propertyTbl;
                }

                gridDialog.Text = dialogText;
                gridDialog.Show();
            }
        }

        private void ultraGrid1_ClickCellButton(object sender, Infragistics.Win.UltraWinGrid.CellEventArgs e)
        {
            if (e.Cell.Column.Key as string == "ExportResults")
            {
                #region Export to Excel
                using (var waitCusor = Common.WaitCursor.UsingCreate(this.ultraGrid1, Common.Patterns.WaitCursorModes.GUI))
                {
                    var results = e.Cell.Row.Cells["Result"].Value as DSEDiagnosticLibrary.IResult;

                    if (results != null)
                    {
                        string excelFileName = null;
                        CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                        //dialog.InitialDirectory = this.ultraTextEditorDiagnosticsFolder.Text ?? @"C:\";
                        dialog.IsFolderPicker = false;
                        dialog.Title = "Choose Export Excel Folder Location";
                        dialog.EnsurePathExists = false;
                        dialog.DefaultExtension = ".xlsx";
                        dialog.EnsurePathExists = true;
                        dialog.EnsureValidNames = true;
                        dialog.Filters.Add(new CommonFileDialogFilter("Excel File", "*.xlsx"));
                        using (var suspend = Common.WaitCursor.UsingSuspend())
                        {
                            if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
                            {
                                excelFileName = dialog.FileName;
                            }
                        }

                        if (!string.IsNullOrEmpty(excelFileName))
                        {
                            System.Threading.Tasks.Task.Factory.StartNew(() =>
                            {
                                var buttonText = e.Cell.Row.Cells["ExportResults"].Value;
                                var cellRow = e.Cell.Row;
                                var threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;

                                try
                                {
                                    cellRow.Cells["ExportResults"].Value = "Running...";
                                    cellRow.Cells["ExportResults"].Activation = Infragistics.Win.UltraWinGrid.Activation.Disabled;

                                    if (results is DSEDiagnosticFileParser.file_cassandra_log4net.LogResults)
                                    {
                                        this._progressionMsgs.Add(new Tuple<int, string>(threadId, "Excel Exporting of \"LogCassandraEvent\"..."));

                                        var exporter = Dexiom.EPPlusExporter.EnumerableExporter.Create(results.Results.Cast<DSEDiagnosticLibrary.LogCassandraEvent>());
                                        var excelPackage = exporter.CreateExcelPackage();

                                        this._progressionMsgs.Add(new Tuple<int, string>(threadId, string.Format("Saving Excel file \"{0}\"...", excelFileName)));

                                        //save the document
                                        excelPackage.SaveAs(new System.IO.FileInfo(excelFileName));
                                    }
                                    else
                                    {
                                        this._progressionMsgs.Add(new Tuple<int, string>(threadId, string.Format("Excel Exporting of \"{0}\"...", results.Results.GetType().Name)));

                                        var exporter = Dexiom.EPPlusExporter.EnumerableExporter.Create(results.Results);
                                        var excelPackage = exporter.CreateExcelPackage();

                                        this._progressionMsgs.Add(new Tuple<int, string>(threadId, string.Format("Saving Excel file \"{0}\"...", excelFileName)));

                                        //save the document
                                        excelPackage.SaveAs(new System.IO.FileInfo(excelFileName));
                                    }

                                    this._progressionMsgs.Add(new Tuple<int, string>(threadId, string.Format("Saved Excel file \"{0}\"", excelFileName)));
                                }
                                catch (System.Exception ex)
                                {
                                    buttonText = string.Format("Failed: {0} \"{1}\"", ex.GetType().Name, ex.Message);
                                    this._progressionMsgs.Add(new Tuple<int, string>(threadId, string.Format("Failed saving of Excel file \"{0}\" with exception \"{1}\" ({2})",
                                                                                                         excelFileName,
                                                                                                         ex.Message,
                                                                                                         ex.GetType().Name)));
                                    throw;
                                }
                                finally
                                {
                                    cellRow.Cells["ExportResults"].Value = buttonText;
                                    cellRow.Cells["ExportResults"].Activation = Infragistics.Win.UltraWinGrid.Activation.ActivateOnly;
                                    this._progressionMsgs.RemoveAll(m => m.Item1 == threadId);
                                }
                            });
                        }
                    }
                }
                #endregion
            }
            else if (e.Cell.Column.Key as string == "ShowLogEventDialog")
            {
                #region Show Log Event Dialog
                var logResults = e.Cell.Row.Cells["Result"].Value as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

                if (logResults != null)
                {
                    var logEventDialog = new FormLogEventGrid(logResults.ResultsTask);
                    logEventDialog.Show();
                }
                #endregion
            }
        }
        #endregion

        #region Timer Events
        int _currentTimerIdx = 0;
        private void timerProgress_Tick(object sender, EventArgs e)
        {

            this._progressionMsgs.Lock();

            if (this._currentTimerIdx < this._progressionMsgs.UnSafe.Count)
            {
                this.ultraStatusBar1.Panels["Progress"].Text = this._progressionMsgs.UnSafe[this._currentTimerIdx++].Item2;
            }

            if (this._currentTimerIdx >= this._progressionMsgs.UnSafe.Count)
            {
                this._currentTimerIdx = 0;
            }

            this._progressionMsgs.UnLock();

            this.SetLoggingStatus(this._lastLogMsg);
        }

        #endregion
        
    }
}

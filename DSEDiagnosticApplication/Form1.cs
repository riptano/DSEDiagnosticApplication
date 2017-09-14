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
	public partial class Form1 : Form
	{
        CancellationTokenSource _cancellationSource;
        Common.WaitCursor _waitCusor = new Common.WaitCursor();

        public Form1()
		{
			InitializeComponent();
        }

        IEnumerable<DSEDiagnosticFileParser.DiagnosticFile> _currentDiagnosticFiles = null;

        private async void button1_Click(object sender, EventArgs e)
        {
            var buttonLabel = this.button1.Text;
            this.button1.Enabled = false;
            this.button1.Text = "Running...";
            this.button3.Enabled = false;
            this.button3.Visible = false;
            this._cancellationSource = new CancellationTokenSource();

            this.button2.Enabled = true;
            this.button2.Text = "Cancel";

            this.ultraTextEditorProcessMapperJSONFile.Enabled = false;
            this.ultraTextEditorDiagnosticsFolder.Enabled = false;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = false;
            this.ultraTextEditorCluster.Enabled = false;
            this.ultraTextEditorDC.Enabled = false;

            var items = await this.RunProcessFile().ConfigureAwait(false);

            this.ultraGrid1.DataSource = this._currentDiagnosticFiles = items;

            this.EnableRunButton(buttonLabel);
        }

        delegate void EnableRunButtondEL(string buttonLabel);


        void EnableRunButton(string buttonLabel)
        {
            if(this.button1.InvokeRequired)
            {
                this.Invoke(new EnableRunButtondEL(this.EnableRunButton), buttonLabel);
                return;
            }

            this.button1.Text = buttonLabel;
            this.button1.Enabled = true;

            this.button3.Enabled = true;
            this.button3.Visible = true;

            this.button2.Enabled = false;
            this.button2.Text = this._cancellationSource == null
                                    ? "Cancel"
                                    : (this._cancellationSource.IsCancellationRequested ? "Canceled" : "Cancel");

            this.button5.Enabled = true;
            this.button5.Visible = true;

            this.ultraTextEditorProcessMapperJSONFile.Enabled = true;
            this.ultraTextEditorDiagnosticsFolder.Enabled = true;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = true;
            this.ultraTextEditorCluster.Enabled = true;
            this.ultraTextEditorDC.Enabled = true;
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
        }

        private string _lastLogMsg = string.Empty;
        private void Instance_OnLoggingEventArgs(DSEDiagnosticLogger.Logger sender, DSEDiagnosticLogger.LoggingEventArgs eventArgs)
        {
            foreach (var logMsg in eventArgs.RenderedLogMessages())
            {
                this._lastLogMsg = logMsg;
            }
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

                if (this.ultraDateTimeEditorStartLog.Value == null
                        || this.ultraDateTimeEditorEndLog.Value == null
                        || (DateTime) this.ultraDateTimeEditorStartLog.Value >= (DateTime) this.ultraDateTimeEditorEndLog.Value)
                {
                    DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRange = null;
                }
                else
                {
                    var logStart = (DateTime)this.ultraDateTimeEditorStartLog.Value;
                    var logEnd = (DateTime)this.ultraDateTimeEditorEndLog.Value;
                    var utcStart = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorStartLog.ButtonsRight[0]).Checked;
                    var utcEnd = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorEndLog.ButtonsRight[0]).Checked;

                    DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRange = new DateTimeOffsetRange(utcStart
                                                                                                                ? new DateTimeOffset(logStart, TimeSpan.Zero)
                                                                                                                : logStart,
                                                                                                            utcEnd
                                                                                                                ? new DateTimeOffset(logEnd, TimeSpan.Zero)
                                                                                                                : logEnd);
                }

                if (!string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
                {
                    DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<DSEDiagnosticFileParser.FileMapper[]>(this.ultraTextEditorProcessMapperJSONFile.Text);
                }

                var diagPath = PathUtils.BuildDirectoryPath(this.ultraTextEditorDiagnosticsFolder.Text);

                var task = await DSEDiagnosticFileParser.DiagnosticFile.ProcessFileWaitable(diagPath, this.ultraTextEditorDC.Text, this.ultraTextEditorCluster.Text, null, this._cancellationSource);

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

                if(this.ultraDateTimeEditorStartLog.Value == null
                        || this.ultraDateTimeEditorEndLog.Value == null
                        || (DateTime)this.ultraDateTimeEditorStartLog.Value >= (DateTime)this.ultraDateTimeEditorEndLog.Value)
                {
                    DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRange = null;
                }
                else
                {
                    var logStart = (DateTime) this.ultraDateTimeEditorStartLog.Value;
                    var logEnd = (DateTime)this.ultraDateTimeEditorEndLog.Value;
                    var utcStart = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorStartLog.ButtonsRight[0]).Checked;
                    var utcEnd = ((Infragistics.Win.UltraWinEditors.StateEditorButton)this.ultraDateTimeEditorEndLog.ButtonsRight[0]).Checked;

                    DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRange = new DateTimeOffsetRange(utcStart
                                                                                                                ? new DateTimeOffset(logStart, TimeSpan.Zero)
                                                                                                                : logStart,
                                                                                                            utcEnd
                                                                                                                ? new DateTimeOffset(logEnd, TimeSpan.Zero)
                                                                                                                : logEnd);
                }

                if (!string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
                {
                    DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<DSEDiagnosticFileParser.FileMapper[]>(this.ultraTextEditorProcessMapperJSONFile.Text);
                }

                var diagPath = PathUtils.BuildDirectoryPath(this.ultraTextEditorDiagnosticsFolder.Text);

                var task = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(diagPath).Result;

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
                }
            }
            else if ((eventArgs.Category & DSEDiagnosticFileParser.ProgressionEventArgs.Categories.End) != 0 || (eventArgs.Category & DSEDiagnosticFileParser.ProgressionEventArgs.Categories.Cancel) != 0)
            {
                var key = (eventArgs.StepName + eventArgs.ThreadId.ToString()).GetHashCode();

                this._progressionMsgs.RemoveAll(i => i.Item1 == key);
            }
        }

        private void DiagnosticFile_OnException(object sender, DSEDiagnosticFileParser.ExceptionEventArgs eventArgs)
        {
            this._progressionMsgs.Add(new Tuple<int, string>(eventArgs.Exception.GetHashCode(), string.Format("Exception: {0}", eventArgs.Exception.Message)));
        }

        private void button2_Click(object sender, EventArgs e)
        {
            if (this._cancellationSource != null)
            {
                this.button2.Enabled = false;
                this._cancellationSource.Cancel();
                this.button2.Text = "Canceling";
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

        private void Form1_Load(object sender, EventArgs e)
        {
            this.ultraTextEditorDiagnosticsFolder.Text = Properties.Settings.Default.DefaultDiagnosticsFolder;
            this.ultraTextEditorProcessMapperJSONFile.Text = null;
        }

        private void ultraTextEditorProcessMapperJSONFile_EditorButtonClick(object sender, Infragistics.Win.UltraWinEditors.EditorButtonEventArgs e)
        {
            if (e.Button.Key == "FileEditor")
            {
                CommonOpenFileDialog dialog = new CommonOpenFileDialog();
                if(string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
                {
                    if(string.IsNullOrEmpty(Properties.Settings.Default.DefaultProcessMapperJSONFile))
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
            else if(e.Button.Key == "StringEditor")
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

                if(dialog.ShowDialog(this) == DialogResult.OK)
                {
                    this.ultraTextEditorProcessMapperJSONFile.Text = dialog.Text;
                }
            }
        }

        private void ultraGrid1_InitializeLayout(object sender, Infragistics.Win.UltraWinGrid.InitializeLayoutEventArgs e)
        {
            var resultCol = e.Layout.Bands[0].Columns["Result"];

            resultCol.CellActivation = Infragistics.Win.UltraWinGrid.Activation.ActivateOnly;
            resultCol.CellAppearance.ForeColor = Color.Blue;
            resultCol.CellAppearance.FontData.Underline = Infragistics.Win.DefaultableBoolean.True;
            resultCol.CellAppearance.Cursor = Cursors.Hand;
        }

        private void ultraGrid1_MouseDown(object sender, System.Windows.Forms.MouseEventArgs e)
        {
            var objClickedElement = this.ultraGrid1.DisplayLayout.UIElement.ElementFromPoint(this.ultraGrid1.PointToClient(Form1.MousePosition));

            if (objClickedElement == null) return;

            var objCellElement = (Infragistics.Win.UltraWinGrid.CellUIElement)objClickedElement.GetAncestor(typeof(Infragistics.Win.UltraWinGrid.CellUIElement));

            if (objCellElement == null) return;

            var objCell = (Infragistics.Win.UltraWinGrid.UltraGridCell)objCellElement.GetContext(typeof(Infragistics.Win.UltraWinGrid.UltraGridCell));

            if (objCell == null) return;

            if (objCell.Column.Key == "Result")
            {
                var gridDialog = new FormObjectGrid();
                var propertyTbl = new PropertyTable();

                //var resultArray = Array.CreateInstance(objCell.Value.GetType(), 2);

                //resultArray.SetValue(objCell.Value, 0);
                //resultArray.SetValue(((DSEDiagnosticLibrary.IResult)objCell.Value).Cluster, 1);
                //

                propertyTbl.Properties.Add(new PropertySpec("Result", objCell.Value.GetType(), "Result"));
                propertyTbl["Result"] = objCell.Value;

                foreach (var prop in objCell.Value.GetType().GetFields())
                {
                    propertyTbl.Properties.Add(new PropertySpec(prop.Name, prop.FieldType, "Result"));
                    propertyTbl[prop.Name] = prop.GetValue(objCell.Value);
                }
                foreach (var prop in objCell.Value.GetType().GetProperties())
                {
                    propertyTbl.Properties.Add(new PropertySpec(prop.Name, prop.PropertyType, "Result"));
                    propertyTbl[prop.Name] = prop.GetValue(objCell.Value);
                }

                var cluster = ((DSEDiagnosticLibrary.IResult)objCell.Value).Cluster;
                propertyTbl.Properties.Add(new PropertySpec("Cluster", typeof(DSEDiagnosticLibrary.Cluster), "Custer"));
                propertyTbl["Cluster"] = cluster;

                foreach (var prop in cluster.GetType().GetFields())
                {
                    propertyTbl.Properties.Add(new PropertySpec(prop.Name, prop.FieldType, "Custer"));
                    propertyTbl[prop.Name] = prop.GetValue(cluster);
                }
                foreach (var prop in cluster.GetType().GetProperties())
                {
                    propertyTbl.Properties.Add(new PropertySpec(prop.Name, prop.PropertyType, "Custer"));
                    propertyTbl[prop.Name] = prop.GetValue(cluster);
                }

                gridDialog.DataSource = propertyTbl;

                gridDialog.Show(this);
            }

        }

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

        private void button3_Click(object sender, EventArgs e)
        {
            var buttonLabel = this.button1.Text;
            this.button1.Enabled = false;
            this.button1.Text = "Sync-Running...";
            this.button3.Enabled = false;
            this.button3.Visible = false;
            this.button5.Enabled = false;
            this.button5.Visible = false;
            //this._cancellationSource = new CancellationTokenSource();

            //this.button2.Enabled = true;
            //this.button2.Text = "Cancel";

            this.ultraTextEditorProcessMapperJSONFile.Enabled = false;
            this.ultraTextEditorDiagnosticsFolder.Enabled = false;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = false;
            this.ultraTextEditorCluster.Enabled = false;
            this.ultraTextEditorDC.Enabled = false;


            var items = this.RunSyncProcessFile();

            this.ultraGrid1.DataSource = this._currentDiagnosticFiles = items;

            this.EnableRunButton(buttonLabel);
        }

        private void button4_Click(object sender, EventArgs e)
        {
            using (var waitCusor = Common.WaitCursor.UsingCreate(this.ultraGrid1, Common.Patterns.WaitCursorModes.GUI))
            {
                DSEDiagnosticLibrary.Cluster.Clear();
                this.ultraGrid1.DataSource = this._currentDiagnosticFiles = null;
            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            var textPad = new System.Diagnostics.Process();
            textPad.StartInfo.FileName = "textpad.exe";
            textPad.StartInfo.Arguments = @".\DSEDiagnosticApplication.log";
            textPad.Start();
        }
    }
}

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

        public Form1()
		{
			InitializeComponent();
        }

        private async void button1_Click(object sender, EventArgs e)
        {
            var buttonLabel = this.button1.Text;
            this.button1.Enabled = false;
            this.button1.Text = "Running...";
            this._cancellationSource = new CancellationTokenSource();

            this.button2.Enabled = true;
            this.button2.Text = "Cancel";

            this.ultraTextEditorProcessMapperJSONFile.Enabled = false;
            this.ultraTextEditorDiagnosticsFolder.Enabled = false;
            this.ultraCheckEditorDisableParallelProcessing.Enabled = false;
            this.ultraTextEditorCluster.Enabled = false;
            this.ultraTextEditorDC.Enabled = false;

            var items = await this.RunProcessFile().ConfigureAwait(false);

            this.ultraGrid1.DataSource = items;

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

            this.button2.Enabled = false;
            this.button2.Text = this._cancellationSource.IsCancellationRequested ? "Canceled" : "Cancel";

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

        private void Instance_OnLoggingEventArgs(DSEDiagnosticLogger.Logger sender, DSEDiagnosticLogger.LoggingEventArgs eventArgs)
        {
            this.SetLoggingStatus(eventArgs.RenderedLogMessages().FirstOrDefault());
        }

        private async Task<IEnumerable<DSEDiagnosticFileParser.DiagnosticFile>> RunProcessFile()
        {
            Logger.Instance.Info("test");
            Logger.Instance.OnLoggingEvent += Instance_OnLoggingEventArgs;
            DSEDiagnosticFileParser.DiagnosticFile.DisableParallelProcessing = this.ultraCheckEditorDisableParallelProcessing.Checked;

            if(!string.IsNullOrEmpty(this.ultraTextEditorProcessMapperJSONFile.Text))
            {
                DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappings = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<DSEDiagnosticFileParser.FileMapper[]>(this.ultraTextEditorProcessMapperJSONFile.Text);
            }

            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\TestUnZip");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Y31169_cluster-diagnostics-2017_01_06_08_02_04_UTC");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\prod_cassandra_4-diagnostics-2017_02_20_08_26_20_UTC");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\20170217\opsc-2017-02-08-09-06-14-CET\AdditionalLogs");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\20170214\usprod");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Optimus-diagnostics-2017_02_06_00_05_33_UTC");
            var diagPath = PathUtils.BuildDirectoryPath(this.ultraTextEditorDiagnosticsFolder.Text);

            var task = await DSEDiagnosticFileParser.DiagnosticFile.ProcessFileWaitable(diagPath, this.ultraTextEditorDC.Text, this.ultraTextEditorCluster.Text, null, this._cancellationSource);

            if(task.Any(t => t.Processed))
            {
                this.SetLoggingStatus("all processed");
            }
            else
            {
                this.SetLoggingStatus("NOT all processed");
            }

            //tasks.ContinueWith(waitingTasks => this.EnableRunButton(buttonLabel));
            //tasks.Wait();

            // var z = DSEDiagnosticLibrary.Cluster.Clusters;
            //Task.WhenAll(tasks).Wait();
            //

            return task;
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
                dialog.DefaultExtension = "json";
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
                var resultArray = Array.CreateInstance(objCell.Value.GetType(), 1);

                resultArray.SetValue(objCell.Value, 0);

                gridDialog.DataSource = resultArray;

                gridDialog.Show(this);
            }

        }

    }
}

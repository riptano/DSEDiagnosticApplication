using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Common;
using Common.Path;

namespace DSEDiagnosticApplication
{
	public partial class Form1 : Form
	{
		public Form1()
		{
			InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            var buttonLabel = this.button1.Text;
            this.button1.Enabled = false;
            this.button1.Text = "Running...";


            Logger.Instance.Info("test");
            Logger.Instance.OnLoggingEvent += Instance_OnLoggingEventArgs;
            DSEDiagnosticFileParser.DiagnosticFile.DisableParallelProcessing = true;
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\TestUnZip");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Y31169_cluster-diagnostics-2017_01_06_08_02_04_UTC");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\prod_cassandra_4-diagnostics-2017_02_20_08_26_20_UTC");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\20170217\opsc-2017-02-08-09-06-14-CET\AdditionalLogs");
            var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\20170214\usprod");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Optimus-diagnostics-2017_02_06_00_05_33_UTC");

            var tasks = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(diagPath);

            //tasks.ContinueWith(waitingTasks => this.EnableRunButton(buttonLabel));
            tasks.Wait();

            // var z = DSEDiagnosticLibrary.Cluster.Clusters;
            //Task.WhenAll(tasks).Wait();

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
        }

        delegate void SetLabelDelegate(string newvalue);

        void SetLabel(string newValue)
        {
            if(this.label1.InvokeRequired)
            {
                this.Invoke(new SetLabelDelegate(this.SetLabel), newValue);
                return;
            }

            this.label1.Text = newValue;
        }

        private void Instance_OnLoggingEventArgs(DSEDiagnosticLogger.Logger sender, DSEDiagnosticLogger.LoggingEventArgs eventArgs)
        {
                        
        }
    }
}

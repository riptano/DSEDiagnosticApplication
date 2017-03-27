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
            Logger.Instance.Info("test");

            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\TestUnZip");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Y31169_cluster-diagnostics-2017_01_06_08_02_04_UTC");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\prod_cassandra_4-diagnostics-2017_02_20_08_26_20_UTC");
            //var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\20170217\opsc-2017-02-08-09-06-14-CET\AdditionalLogs");
            var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Optimus-diagnostics-2017_02_06_00_05_33_UTC");

            var tasks = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(diagPath);

            tasks.Wait();

            var z = DSEDiagnosticLibrary.Cluster.MasterCluster;
            //Task.WhenAll(tasks).Wait();
        }
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DSEDiagnosticApplication
{
    public partial class FormJSONEditor : Form
    {
        public FormJSONEditor()
        {
            InitializeComponent();
        }

        public override string Text
        {
            get { return this.ultraTextEditorJSONString?.Text; }
            set { this.ultraTextEditorJSONString.Text = value; }
        }

        private void FormJSONEditor_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (this.DialogResult == DialogResult.OK)
            {
                var jsonStr = this.ultraTextEditorJSONString.Text;

                if (!string.IsNullOrEmpty(jsonStr))
                {
                    if (jsonStr[0] == '"' && jsonStr.Last() == '"')
                    {
                        jsonStr = jsonStr.Substring(1, jsonStr.Length - 2);
                    }

                    if (this.ultraCheckEditorFmtEscape.Checked)
                    {
                        jsonStr = jsonStr.Replace(@"\\", @"\");
                    }

                    this.ultraTextEditorJSONString.Text = jsonStr;
                }
            }
        }
    }
}

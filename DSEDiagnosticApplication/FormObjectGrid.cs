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
    public partial class FormObjectGrid : Form
    {
        public FormObjectGrid()
        {
            InitializeComponent();
        }

        public object DataSource
        {
            get { return this.propertyGrid1?.SelectedObject; }
            set
            {
                if(value != this.propertyGrid1.SelectedObject)
                {
                    if(value == null)
                    {
                        this.Text = "Object Grid";
                    }
                    else
                    {
                        this.Text = value.GetType().Name + " Grid";
                    }
                    //if (value.GetType().IsArray)
                    //{
                    //    this.propertyGrid1.SelectedObjects = (object[]) value;
                    //}
                    //else
                    {
                        this.propertyGrid1.SelectedObject = value;
                    }
                }
            }
        }

        private void buttonOK_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        

    }
}

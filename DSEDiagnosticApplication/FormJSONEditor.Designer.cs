namespace DSEDiagnosticApplication
{
    partial class FormJSONEditor
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.ultraTextEditorJSONString = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.buttonOK = new System.Windows.Forms.Button();
            this.button2 = new System.Windows.Forms.Button();
            this.ultraCheckEditorFmtEscape = new Infragistics.Win.UltraWinEditors.UltraCheckEditor();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorJSONString)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorFmtEscape)).BeginInit();
            this.SuspendLayout();
            // 
            // ultraTextEditorJSONString
            // 
            this.ultraTextEditorJSONString.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraTextEditorJSONString.Location = new System.Drawing.Point(1, 0);
            this.ultraTextEditorJSONString.Multiline = true;
            this.ultraTextEditorJSONString.Name = "ultraTextEditorJSONString";
            this.ultraTextEditorJSONString.Scrollbars = System.Windows.Forms.ScrollBars.Both;
            this.ultraTextEditorJSONString.ShowOverflowIndicator = true;
            this.ultraTextEditorJSONString.Size = new System.Drawing.Size(305, 225);
            this.ultraTextEditorJSONString.TabIndex = 0;
            // 
            // buttonOK
            // 
            this.buttonOK.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.buttonOK.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.buttonOK.Location = new System.Drawing.Point(12, 231);
            this.buttonOK.Name = "buttonOK";
            this.buttonOK.Size = new System.Drawing.Size(75, 23);
            this.buttonOK.TabIndex = 1;
            this.buttonOK.Text = "O&K";
            this.buttonOK.UseVisualStyleBackColor = true;
            // 
            // button2
            // 
            this.button2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.button2.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.button2.Location = new System.Drawing.Point(93, 231);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(75, 23);
            this.button2.TabIndex = 2;
            this.button2.Text = "&Cancel";
            this.button2.UseVisualStyleBackColor = true;
            // 
            // ultraCheckEditorFmtEscape
            // 
            this.ultraCheckEditorFmtEscape.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraCheckEditorFmtEscape.Location = new System.Drawing.Point(213, 231);
            this.ultraCheckEditorFmtEscape.Name = "ultraCheckEditorFmtEscape";
            this.ultraCheckEditorFmtEscape.Size = new System.Drawing.Size(82, 20);
            this.ultraCheckEditorFmtEscape.TabIndex = 3;
            this.ultraCheckEditorFmtEscape.Text = "Fix Escape";
            // 
            // FormJSONEditor
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(307, 257);
            this.Controls.Add(this.ultraCheckEditorFmtEscape);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.buttonOK);
            this.Controls.Add(this.ultraTextEditorJSONString);
            this.Name = "FormJSONEditor";
            this.Text = "JSON Editor";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FormJSONEditor_FormClosing);
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorJSONString)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorFmtEscape)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorJSONString;
        private System.Windows.Forms.Button buttonOK;
        private System.Windows.Forms.Button button2;
        private Infragistics.Win.UltraWinEditors.UltraCheckEditor ultraCheckEditorFmtEscape;
    }
}
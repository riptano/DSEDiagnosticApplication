namespace DSEDiagnosticApplication
{
	partial class Form1
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
            this.components = new System.ComponentModel.Container();
            Infragistics.Win.Appearance appearance1 = new Infragistics.Win.Appearance();
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand1 = new Infragistics.Win.UltraWinGrid.UltraGridBand("IResult", -1);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn1 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Path");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn2 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Cluster");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn3 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("DataCenter");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn4 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Node");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn5 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("NbrItems");
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn6 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("Results");
            Infragistics.Win.UltraWinGrid.UltraGridBand ultraGridBand2 = new Infragistics.Win.UltraWinGrid.UltraGridBand("Results", -1);
            Infragistics.Win.Appearance appearance2 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance3 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance4 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance5 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance6 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance7 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance8 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance9 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance10 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance11 = new Infragistics.Win.Appearance();
            Infragistics.Win.Appearance appearance12 = new Infragistics.Win.Appearance();
            Infragistics.Win.UltraWinEditors.EditorButton editorButton1 = new Infragistics.Win.UltraWinEditors.EditorButton("DirEditor");
            Infragistics.Win.UltraWinEditors.EditorButton editorButton2 = new Infragistics.Win.UltraWinEditors.EditorButton("FileEditor");
            Infragistics.Win.UltraWinEditors.EditorButton editorButton3 = new Infragistics.Win.UltraWinEditors.EditorButton("StringEditor");
            Infragistics.Win.UltraWinStatusBar.UltraStatusPanel ultraStatusPanel1 = new Infragistics.Win.UltraWinStatusBar.UltraStatusPanel();
            Infragistics.Win.UltraWinStatusBar.UltraStatusPanel ultraStatusPanel2 = new Infragistics.Win.UltraWinStatusBar.UltraStatusPanel();
            Infragistics.Win.UltraWinEditors.StateEditorButton stateEditorButton1 = new Infragistics.Win.UltraWinEditors.StateEditorButton("UTCStart");
            Infragistics.Win.UltraWinEditors.StateEditorButton stateEditorButton2 = new Infragistics.Win.UltraWinEditors.StateEditorButton("UTCEnd");
            this.button1 = new System.Windows.Forms.Button();
            this.button2 = new System.Windows.Forms.Button();
            this.ultraGrid1 = new Infragistics.Win.UltraWinGrid.UltraGrid();
            this.ultraGroupBox1 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraTextEditorCluster = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraGroupBox2 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraTextEditorDC = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraGroupBox3 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraTextEditorDiagnosticsFolder = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraTextEditorProcessMapperJSONFile = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraGroupBox4 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraCheckEditorDisableParallelProcessing = new Infragistics.Win.UltraWinEditors.UltraCheckEditor();
            this.ultraStatusBar1 = new Infragistics.Win.UltraWinStatusBar.UltraStatusBar();
            this.timerProgress = new System.Windows.Forms.Timer(this.components);
            this.button3 = new System.Windows.Forms.Button();
            this.button4 = new System.Windows.Forms.Button();
            this.button5 = new System.Windows.Forms.Button();
            this.ultraGroupBox5 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraGroupBox6 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraDateTimeEditorStartLog = new Infragistics.Win.UltraWinEditors.UltraDateTimeEditor();
            this.ultraDateTimeEditorEndLog = new Infragistics.Win.UltraWinEditors.UltraDateTimeEditor();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGrid1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox1)).BeginInit();
            this.ultraGroupBox1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorCluster)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox2)).BeginInit();
            this.ultraGroupBox2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorDC)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox3)).BeginInit();
            this.ultraGroupBox3.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorDiagnosticsFolder)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorProcessMapperJSONFile)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox4)).BeginInit();
            this.ultraGroupBox4.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorDisableParallelProcessing)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraStatusBar1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox5)).BeginInit();
            this.ultraGroupBox5.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox6)).BeginInit();
            this.ultraGroupBox6.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorStartLog)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorEndLog)).BeginInit();
            this.SuspendLayout();
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(23, 22);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 0;
            this.button1.Text = "Execute";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // button2
            // 
            this.button2.Enabled = false;
            this.button2.Location = new System.Drawing.Point(118, 22);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(75, 23);
            this.button2.TabIndex = 2;
            this.button2.Text = "Cancel";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // ultraGrid1
            // 
            this.ultraGrid1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            appearance1.BackColor = System.Drawing.SystemColors.Window;
            appearance1.BorderColor = System.Drawing.SystemColors.InactiveCaption;
            this.ultraGrid1.DisplayLayout.Appearance = appearance1;
            this.ultraGrid1.DisplayLayout.AutoFitStyle = Infragistics.Win.UltraWinGrid.AutoFitStyle.ResizeAllColumns;
            ultraGridColumn1.Header.VisiblePosition = 0;
            ultraGridColumn1.Width = 212;
            ultraGridColumn2.Header.VisiblePosition = 1;
            ultraGridColumn2.Width = 207;
            ultraGridColumn3.Header.VisiblePosition = 2;
            ultraGridColumn3.Width = 207;
            ultraGridColumn4.Header.VisiblePosition = 3;
            ultraGridColumn4.Width = 207;
            ultraGridColumn5.Header.VisiblePosition = 4;
            ultraGridColumn5.Width = 207;
            ultraGridColumn6.Header.VisiblePosition = 5;
            ultraGridColumn6.Width = 207;
            ultraGridBand1.Columns.AddRange(new object[] {
            ultraGridColumn1,
            ultraGridColumn2,
            ultraGridColumn3,
            ultraGridColumn4,
            ultraGridColumn5,
            ultraGridColumn6});
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand1);
            this.ultraGrid1.DisplayLayout.BandsSerializer.Add(ultraGridBand2);
            this.ultraGrid1.DisplayLayout.BorderStyle = Infragistics.Win.UIElementBorderStyle.Solid;
            this.ultraGrid1.DisplayLayout.CaptionVisible = Infragistics.Win.DefaultableBoolean.False;
            appearance2.BackColor = System.Drawing.SystemColors.ActiveBorder;
            appearance2.BackColor2 = System.Drawing.SystemColors.ControlDark;
            appearance2.BackGradientStyle = Infragistics.Win.GradientStyle.Vertical;
            appearance2.BorderColor = System.Drawing.SystemColors.Window;
            this.ultraGrid1.DisplayLayout.GroupByBox.Appearance = appearance2;
            appearance3.ForeColor = System.Drawing.SystemColors.GrayText;
            this.ultraGrid1.DisplayLayout.GroupByBox.BandLabelAppearance = appearance3;
            this.ultraGrid1.DisplayLayout.GroupByBox.BorderStyle = Infragistics.Win.UIElementBorderStyle.Solid;
            appearance4.BackColor = System.Drawing.SystemColors.ControlLightLight;
            appearance4.BackColor2 = System.Drawing.SystemColors.Control;
            appearance4.BackGradientStyle = Infragistics.Win.GradientStyle.Horizontal;
            appearance4.ForeColor = System.Drawing.SystemColors.GrayText;
            this.ultraGrid1.DisplayLayout.GroupByBox.PromptAppearance = appearance4;
            this.ultraGrid1.DisplayLayout.MaxColScrollRegions = 1;
            this.ultraGrid1.DisplayLayout.MaxRowScrollRegions = 1;
            appearance5.BackColor = System.Drawing.SystemColors.Window;
            appearance5.ForeColor = System.Drawing.SystemColors.ControlText;
            this.ultraGrid1.DisplayLayout.Override.ActiveCellAppearance = appearance5;
            appearance6.BackColor = System.Drawing.SystemColors.Highlight;
            appearance6.ForeColor = System.Drawing.SystemColors.HighlightText;
            this.ultraGrid1.DisplayLayout.Override.ActiveRowAppearance = appearance6;
            this.ultraGrid1.DisplayLayout.Override.AllowAddNew = Infragistics.Win.UltraWinGrid.AllowAddNew.No;
            this.ultraGrid1.DisplayLayout.Override.AllowDelete = Infragistics.Win.DefaultableBoolean.False;
            this.ultraGrid1.DisplayLayout.Override.AllowRowFiltering = Infragistics.Win.DefaultableBoolean.True;
            this.ultraGrid1.DisplayLayout.Override.AllowUpdate = Infragistics.Win.DefaultableBoolean.False;
            this.ultraGrid1.DisplayLayout.Override.BorderStyleCell = Infragistics.Win.UIElementBorderStyle.Dotted;
            this.ultraGrid1.DisplayLayout.Override.BorderStyleRow = Infragistics.Win.UIElementBorderStyle.Dotted;
            appearance7.BackColor = System.Drawing.SystemColors.Window;
            this.ultraGrid1.DisplayLayout.Override.CardAreaAppearance = appearance7;
            appearance8.BorderColor = System.Drawing.Color.Silver;
            appearance8.TextTrimming = Infragistics.Win.TextTrimming.EllipsisCharacter;
            this.ultraGrid1.DisplayLayout.Override.CellAppearance = appearance8;
            this.ultraGrid1.DisplayLayout.Override.CellClickAction = Infragistics.Win.UltraWinGrid.CellClickAction.EditAndSelectText;
            this.ultraGrid1.DisplayLayout.Override.CellPadding = 0;
            appearance9.BackColor = System.Drawing.SystemColors.Control;
            appearance9.BackColor2 = System.Drawing.SystemColors.ControlDark;
            appearance9.BackGradientAlignment = Infragistics.Win.GradientAlignment.Element;
            appearance9.BackGradientStyle = Infragistics.Win.GradientStyle.Horizontal;
            appearance9.BorderColor = System.Drawing.SystemColors.Window;
            this.ultraGrid1.DisplayLayout.Override.GroupByRowAppearance = appearance9;
            appearance10.TextHAlignAsString = "Left";
            this.ultraGrid1.DisplayLayout.Override.HeaderAppearance = appearance10;
            this.ultraGrid1.DisplayLayout.Override.HeaderClickAction = Infragistics.Win.UltraWinGrid.HeaderClickAction.SortMulti;
            this.ultraGrid1.DisplayLayout.Override.HeaderStyle = Infragistics.Win.HeaderStyle.WindowsXPCommand;
            appearance11.BackColor = System.Drawing.SystemColors.Window;
            appearance11.BorderColor = System.Drawing.Color.Silver;
            this.ultraGrid1.DisplayLayout.Override.RowAppearance = appearance11;
            this.ultraGrid1.DisplayLayout.Override.RowSelectorHeaderStyle = Infragistics.Win.UltraWinGrid.RowSelectorHeaderStyle.ColumnChooserButton;
            this.ultraGrid1.DisplayLayout.Override.RowSelectors = Infragistics.Win.DefaultableBoolean.True;
            this.ultraGrid1.DisplayLayout.Override.SupportDataErrorInfo = Infragistics.Win.UltraWinGrid.SupportDataErrorInfo.CellsOnly;
            appearance12.BackColor = System.Drawing.SystemColors.ControlLight;
            this.ultraGrid1.DisplayLayout.Override.TemplateAddRowAppearance = appearance12;
            this.ultraGrid1.DisplayLayout.ScrollBounds = Infragistics.Win.UltraWinGrid.ScrollBounds.ScrollToFill;
            this.ultraGrid1.DisplayLayout.ScrollStyle = Infragistics.Win.UltraWinGrid.ScrollStyle.Immediate;
            this.ultraGrid1.DisplayLayout.ViewStyleBand = Infragistics.Win.UltraWinGrid.ViewStyleBand.OutlookGroupBy;
            this.ultraGrid1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.ultraGrid1.Location = new System.Drawing.Point(2, 135);
            this.ultraGrid1.Name = "ultraGrid1";
            this.ultraGrid1.Size = new System.Drawing.Size(1285, 400);
            this.ultraGrid1.TabIndex = 3;
            this.ultraGrid1.Text = "Result";
            this.ultraGrid1.InitializeLayout += new Infragistics.Win.UltraWinGrid.InitializeLayoutEventHandler(this.ultraGrid1_InitializeLayout);
            this.ultraGrid1.MouseDown += new System.Windows.Forms.MouseEventHandler(this.ultraGrid1_MouseDown);
            // 
            // ultraGroupBox1
            // 
            this.ultraGroupBox1.Controls.Add(this.ultraTextEditorCluster);
            this.ultraGroupBox1.Location = new System.Drawing.Point(218, 22);
            this.ultraGroupBox1.Name = "ultraGroupBox1";
            this.ultraGroupBox1.Size = new System.Drawing.Size(200, 44);
            this.ultraGroupBox1.TabIndex = 4;
            this.ultraGroupBox1.Text = "Default Cluster";
            // 
            // ultraTextEditorCluster
            // 
            this.ultraTextEditorCluster.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorCluster.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorCluster.Name = "ultraTextEditorCluster";
            this.ultraTextEditorCluster.ShowOverflowIndicator = true;
            this.ultraTextEditorCluster.Size = new System.Drawing.Size(194, 21);
            this.ultraTextEditorCluster.TabIndex = 0;
            // 
            // ultraGroupBox2
            // 
            this.ultraGroupBox2.Controls.Add(this.ultraTextEditorDC);
            this.ultraGroupBox2.Location = new System.Drawing.Point(442, 22);
            this.ultraGroupBox2.Name = "ultraGroupBox2";
            this.ultraGroupBox2.Size = new System.Drawing.Size(200, 44);
            this.ultraGroupBox2.TabIndex = 5;
            this.ultraGroupBox2.Text = "Default DC";
            // 
            // ultraTextEditorDC
            // 
            this.ultraTextEditorDC.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorDC.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorDC.Name = "ultraTextEditorDC";
            this.ultraTextEditorDC.ShowOverflowIndicator = true;
            this.ultraTextEditorDC.Size = new System.Drawing.Size(194, 21);
            this.ultraTextEditorDC.TabIndex = 0;
            // 
            // ultraGroupBox3
            // 
            this.ultraGroupBox3.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraGroupBox3.Controls.Add(this.ultraTextEditorDiagnosticsFolder);
            this.ultraGroupBox3.Location = new System.Drawing.Point(658, 12);
            this.ultraGroupBox3.Name = "ultraGroupBox3";
            this.ultraGroupBox3.Size = new System.Drawing.Size(623, 44);
            this.ultraGroupBox3.TabIndex = 6;
            this.ultraGroupBox3.Text = "Diagnostics Directory";
            // 
            // ultraTextEditorDiagnosticsFolder
            // 
            editorButton1.Key = "DirEditor";
            editorButton1.Text = "...";
            this.ultraTextEditorDiagnosticsFolder.ButtonsRight.Add(editorButton1);
            this.ultraTextEditorDiagnosticsFolder.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorDiagnosticsFolder.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorDiagnosticsFolder.Name = "ultraTextEditorDiagnosticsFolder";
            this.ultraTextEditorDiagnosticsFolder.ShowOverflowIndicator = true;
            this.ultraTextEditorDiagnosticsFolder.Size = new System.Drawing.Size(617, 21);
            this.ultraTextEditorDiagnosticsFolder.TabIndex = 0;
            this.ultraTextEditorDiagnosticsFolder.EditorButtonClick += new Infragistics.Win.UltraWinEditors.EditorButtonEventHandler(this.ultraTextEditorDiagnosticsFolder_EditorButtonClick);
            // 
            // ultraTextEditorProcessMapperJSONFile
            // 
            editorButton2.Key = "FileEditor";
            editorButton2.Text = "...";
            editorButton3.Key = "StringEditor";
            editorButton3.Text = "J";
            this.ultraTextEditorProcessMapperJSONFile.ButtonsRight.Add(editorButton2);
            this.ultraTextEditorProcessMapperJSONFile.ButtonsRight.Add(editorButton3);
            this.ultraTextEditorProcessMapperJSONFile.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorProcessMapperJSONFile.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorProcessMapperJSONFile.Multiline = true;
            this.ultraTextEditorProcessMapperJSONFile.Name = "ultraTextEditorProcessMapperJSONFile";
            this.ultraTextEditorProcessMapperJSONFile.ShowOverflowIndicator = true;
            this.ultraTextEditorProcessMapperJSONFile.Size = new System.Drawing.Size(619, 25);
            this.ultraTextEditorProcessMapperJSONFile.TabIndex = 0;
            this.ultraTextEditorProcessMapperJSONFile.EditorButtonClick += new Infragistics.Win.UltraWinEditors.EditorButtonEventHandler(this.ultraTextEditorProcessMapperJSONFile_EditorButtonClick);
            // 
            // ultraGroupBox4
            // 
            this.ultraGroupBox4.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraGroupBox4.Controls.Add(this.ultraTextEditorProcessMapperJSONFile);
            this.ultraGroupBox4.Location = new System.Drawing.Point(659, 62);
            this.ultraGroupBox4.Name = "ultraGroupBox4";
            this.ultraGroupBox4.Size = new System.Drawing.Size(625, 44);
            this.ultraGroupBox4.TabIndex = 7;
            this.ultraGroupBox4.Text = "Process Mapper JSON File or string";
            // 
            // ultraCheckEditorDisableParallelProcessing
            // 
            this.ultraCheckEditorDisableParallelProcessing.Location = new System.Drawing.Point(118, 81);
            this.ultraCheckEditorDisableParallelProcessing.Name = "ultraCheckEditorDisableParallelProcessing";
            this.ultraCheckEditorDisableParallelProcessing.Size = new System.Drawing.Size(95, 48);
            this.ultraCheckEditorDisableParallelProcessing.TabIndex = 8;
            this.ultraCheckEditorDisableParallelProcessing.Text = "Disable Parallel Processing";
            // 
            // ultraStatusBar1
            // 
            this.ultraStatusBar1.Location = new System.Drawing.Point(0, 541);
            this.ultraStatusBar1.Name = "ultraStatusBar1";
            ultraStatusPanel1.Key = "Progress";
            ultraStatusPanel1.MinWidth = 100;
            ultraStatusPanel1.SizingMode = Infragistics.Win.UltraWinStatusBar.PanelSizingMode.Spring;
            ultraStatusPanel1.Style = Infragistics.Win.UltraWinStatusBar.PanelStyle.AutoStatusText;
            ultraStatusPanel1.Width = 300;
            ultraStatusPanel2.Key = "LoggingStatus";
            ultraStatusPanel2.SizingMode = Infragistics.Win.UltraWinStatusBar.PanelSizingMode.Automatic;
            ultraStatusPanel2.ToolTipText = "Logging Status";
            this.ultraStatusBar1.Panels.AddRange(new Infragistics.Win.UltraWinStatusBar.UltraStatusPanel[] {
            ultraStatusPanel1,
            ultraStatusPanel2});
            this.ultraStatusBar1.Size = new System.Drawing.Size(1296, 25);
            this.ultraStatusBar1.TabIndex = 9;
            this.ultraStatusBar1.Text = "ultraStatusBar1";
            // 
            // timerProgress
            // 
            this.timerProgress.Enabled = true;
            this.timerProgress.Interval = 1500;
            this.timerProgress.Tick += new System.EventHandler(this.timerProgress_Tick);
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(23, 51);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(75, 23);
            this.button3.TabIndex = 10;
            this.button3.Text = "Sync-Execute";
            this.button3.UseVisualStyleBackColor = true;
            this.button3.Click += new System.EventHandler(this.button3_Click);
            // 
            // button4
            // 
            this.button4.Location = new System.Drawing.Point(118, 51);
            this.button4.Name = "button4";
            this.button4.Size = new System.Drawing.Size(75, 23);
            this.button4.TabIndex = 11;
            this.button4.Text = "Clear";
            this.button4.UseVisualStyleBackColor = true;
            this.button4.Click += new System.EventHandler(this.button4_Click);
            // 
            // button5
            // 
            this.button5.Location = new System.Drawing.Point(23, 93);
            this.button5.Name = "button5";
            this.button5.Size = new System.Drawing.Size(75, 23);
            this.button5.TabIndex = 12;
            this.button5.Text = "Open Log";
            this.button5.UseVisualStyleBackColor = true;
            this.button5.Click += new System.EventHandler(this.button5_Click);
            // 
            // ultraGroupBox5
            // 
            this.ultraGroupBox5.Controls.Add(this.ultraDateTimeEditorStartLog);
            this.ultraGroupBox5.Location = new System.Drawing.Point(215, 81);
            this.ultraGroupBox5.Name = "ultraGroupBox5";
            this.ultraGroupBox5.Size = new System.Drawing.Size(200, 44);
            this.ultraGroupBox5.TabIndex = 5;
            this.ultraGroupBox5.Text = "Log Beginning Timeframe";
            // 
            // ultraGroupBox6
            // 
            this.ultraGroupBox6.Controls.Add(this.ultraDateTimeEditorEndLog);
            this.ultraGroupBox6.Location = new System.Drawing.Point(439, 81);
            this.ultraGroupBox6.Name = "ultraGroupBox6";
            this.ultraGroupBox6.Size = new System.Drawing.Size(200, 44);
            this.ultraGroupBox6.TabIndex = 6;
            this.ultraGroupBox6.Text = "Log Ending Timeframe";
            // 
            // ultraDateTimeEditorStartLog
            // 
            stateEditorButton1.Key = "UTCStart";
            stateEditorButton1.Text = "UTC";
            this.ultraDateTimeEditorStartLog.ButtonsRight.Add(stateEditorButton1);
            this.ultraDateTimeEditorStartLog.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraDateTimeEditorStartLog.Location = new System.Drawing.Point(3, 16);
            this.ultraDateTimeEditorStartLog.Name = "ultraDateTimeEditorStartLog";
            this.ultraDateTimeEditorStartLog.Size = new System.Drawing.Size(194, 21);
            this.ultraDateTimeEditorStartLog.SpinButtonDisplayStyle = Infragistics.Win.ButtonDisplayStyle.Always;
            this.ultraDateTimeEditorStartLog.TabIndex = 0;
            // 
            // ultraDateTimeEditorEndLog
            // 
            stateEditorButton2.Key = "UTCEnd";
            stateEditorButton2.Text = "UTC";
            this.ultraDateTimeEditorEndLog.ButtonsRight.Add(stateEditorButton2);
            this.ultraDateTimeEditorEndLog.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraDateTimeEditorEndLog.Location = new System.Drawing.Point(3, 16);
            this.ultraDateTimeEditorEndLog.Name = "ultraDateTimeEditorEndLog";
            this.ultraDateTimeEditorEndLog.Size = new System.Drawing.Size(194, 21);
            this.ultraDateTimeEditorEndLog.SpinButtonDisplayStyle = Infragistics.Win.ButtonDisplayStyle.Always;
            this.ultraDateTimeEditorEndLog.TabIndex = 1;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1296, 566);
            this.Controls.Add(this.ultraGroupBox6);
            this.Controls.Add(this.ultraGroupBox5);
            this.Controls.Add(this.button5);
            this.Controls.Add(this.button4);
            this.Controls.Add(this.button3);
            this.Controls.Add(this.ultraStatusBar1);
            this.Controls.Add(this.ultraCheckEditorDisableParallelProcessing);
            this.Controls.Add(this.ultraGroupBox4);
            this.Controls.Add(this.ultraGroupBox3);
            this.Controls.Add(this.ultraGroupBox2);
            this.Controls.Add(this.ultraGroupBox1);
            this.Controls.Add(this.ultraGrid1);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.button1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.ultraGrid1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox1)).EndInit();
            this.ultraGroupBox1.ResumeLayout(false);
            this.ultraGroupBox1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorCluster)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox2)).EndInit();
            this.ultraGroupBox2.ResumeLayout(false);
            this.ultraGroupBox2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorDC)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox3)).EndInit();
            this.ultraGroupBox3.ResumeLayout(false);
            this.ultraGroupBox3.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorDiagnosticsFolder)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorProcessMapperJSONFile)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox4)).EndInit();
            this.ultraGroupBox4.ResumeLayout(false);
            this.ultraGroupBox4.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorDisableParallelProcessing)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraStatusBar1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox5)).EndInit();
            this.ultraGroupBox5.ResumeLayout(false);
            this.ultraGroupBox5.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox6)).EndInit();
            this.ultraGroupBox6.ResumeLayout(false);
            this.ultraGroupBox6.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorStartLog)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorEndLog)).EndInit();
            this.ResumeLayout(false);

		}

        #endregion

        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Button button2;
        private Infragistics.Win.UltraWinGrid.UltraGrid ultraGrid1;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox1;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorCluster;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox2;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorDC;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox3;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorDiagnosticsFolder;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorProcessMapperJSONFile;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox4;
        private Infragistics.Win.UltraWinEditors.UltraCheckEditor ultraCheckEditorDisableParallelProcessing;
        private Infragistics.Win.UltraWinStatusBar.UltraStatusBar ultraStatusBar1;
        private System.Windows.Forms.Timer timerProgress;
        private System.Windows.Forms.Button button3;
        private System.Windows.Forms.Button button4;
        private System.Windows.Forms.Button button5;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox5;
        private Infragistics.Win.UltraWinEditors.UltraDateTimeEditor ultraDateTimeEditorStartLog;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox6;
        private Infragistics.Win.UltraWinEditors.UltraDateTimeEditor ultraDateTimeEditorEndLog;
    }
}


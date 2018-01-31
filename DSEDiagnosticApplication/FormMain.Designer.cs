namespace DSEDiagnosticApplication
{
	partial class FormMain
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
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn8 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("ExportResults", 0);
            Infragistics.Win.UltraWinGrid.UltraGridColumn ultraGridColumn7 = new Infragistics.Win.UltraWinGrid.UltraGridColumn("ShowLogEventDialog", 1);
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
            Infragistics.Win.UltraWinStatusBar.UltraStatusPanel ultraStatusPanel3 = new Infragistics.Win.UltraWinStatusBar.UltraStatusPanel();
            Infragistics.Win.UltraWinEditors.StateEditorButton stateEditorButton1 = new Infragistics.Win.UltraWinEditors.StateEditorButton("UTCStart");
            Infragistics.Win.UltraWinEditors.StateEditorButton stateEditorButton2 = new Infragistics.Win.UltraWinEditors.StateEditorButton("UTCEnd");
            Infragistics.Win.UltraWinEditors.EditorButton editorButton4 = new Infragistics.Win.UltraWinEditors.EditorButton("FileEditor");
            Infragistics.Win.UltraWinEditors.EditorButton editorButton5 = new Infragistics.Win.UltraWinEditors.EditorButton("FileEditor");
            Infragistics.Win.UltraWinEditors.EditorButton editorButton6 = new Infragistics.Win.UltraWinEditors.EditorButton("FileEditor");
            this.buttonAsyncExcute = new System.Windows.Forms.Button();
            this.buttonCancelExecution = new System.Windows.Forms.Button();
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
            this.buttonSyncExcute = new System.Windows.Forms.Button();
            this.buttonClearResults = new System.Windows.Forms.Button();
            this.buttonOpenLogFile = new System.Windows.Forms.Button();
            this.ultraGroupBox5 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraDateTimeEditorStartLog = new Infragistics.Win.UltraWinEditors.UltraDateTimeEditor();
            this.ultraGroupBox6 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraDateTimeEditorEndLog = new Infragistics.Win.UltraWinEditors.UltraDateTimeEditor();
            this.ultraGroupBox7 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraTextEditorAdditionalLogs = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraGroupBox8 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraTextEditorAdditionalDDLFiles = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraGroupBox9 = new Infragistics.Win.Misc.UltraGroupBox();
            this.ultraTextEditorAltCompFiles = new Infragistics.Win.UltraWinEditors.UltraTextEditor();
            this.ultraCheckEditorLogDisableMryMap = new Infragistics.Win.UltraWinEditors.UltraCheckEditor();
            this.ultraCheckEditorDisableSysDSEDDL = new Infragistics.Win.UltraWinEditors.UltraCheckEditor();
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
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorStartLog)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox6)).BeginInit();
            this.ultraGroupBox6.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorEndLog)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox7)).BeginInit();
            this.ultraGroupBox7.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorAdditionalLogs)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox8)).BeginInit();
            this.ultraGroupBox8.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorAdditionalDDLFiles)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox9)).BeginInit();
            this.ultraGroupBox9.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorAltCompFiles)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorLogDisableMryMap)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorDisableSysDSEDDL)).BeginInit();
            this.SuspendLayout();
            // 
            // buttonAsyncExcute
            // 
            this.buttonAsyncExcute.Location = new System.Drawing.Point(23, 22);
            this.buttonAsyncExcute.Name = "buttonAsyncExcute";
            this.buttonAsyncExcute.Size = new System.Drawing.Size(75, 23);
            this.buttonAsyncExcute.TabIndex = 0;
            this.buttonAsyncExcute.Text = "Execute";
            this.buttonAsyncExcute.UseVisualStyleBackColor = true;
            this.buttonAsyncExcute.Click += new System.EventHandler(this.buttonAsyncExec_Click);
            // 
            // buttonCancelExecution
            // 
            this.buttonCancelExecution.Enabled = false;
            this.buttonCancelExecution.Location = new System.Drawing.Point(118, 22);
            this.buttonCancelExecution.Name = "buttonCancelExecution";
            this.buttonCancelExecution.Size = new System.Drawing.Size(75, 23);
            this.buttonCancelExecution.TabIndex = 2;
            this.buttonCancelExecution.Text = "Cancel";
            this.buttonCancelExecution.UseVisualStyleBackColor = true;
            this.buttonCancelExecution.Click += new System.EventHandler(this.buttonCancelExcution_Click);
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
            ultraGridColumn1.Width = 168;
            ultraGridColumn2.Header.VisiblePosition = 1;
            ultraGridColumn2.Width = 171;
            ultraGridColumn3.Header.VisiblePosition = 2;
            ultraGridColumn3.Width = 171;
            ultraGridColumn4.Header.VisiblePosition = 3;
            ultraGridColumn4.Width = 171;
            ultraGridColumn5.Header.VisiblePosition = 4;
            ultraGridColumn5.Width = 171;
            ultraGridColumn6.Header.VisiblePosition = 5;
            ultraGridColumn6.Width = 171;
            ultraGridColumn8.AllowGroupBy = Infragistics.Win.DefaultableBoolean.False;
            ultraGridColumn8.AllowRowFiltering = Infragistics.Win.DefaultableBoolean.False;
            ultraGridColumn8.AllowRowSummaries = Infragistics.Win.UltraWinGrid.AllowRowSummaries.False;
            ultraGridColumn8.AutoCompleteMode = Infragistics.Win.AutoCompleteMode.None;
            ultraGridColumn8.CellActivation = Infragistics.Win.UltraWinGrid.Activation.ActivateOnly;
            ultraGridColumn8.DefaultCellValue = "Export";
            ultraGridColumn8.Header.Caption = "Export";
            ultraGridColumn8.Header.ToolTipText = "Export Results";
            ultraGridColumn8.Header.VisiblePosition = 6;
            ultraGridColumn8.Style = Infragistics.Win.UltraWinGrid.ColumnStyle.Button;
            ultraGridColumn8.Width = 107;
            ultraGridColumn7.AllowGroupBy = Infragistics.Win.DefaultableBoolean.False;
            ultraGridColumn7.AllowRowFiltering = Infragistics.Win.DefaultableBoolean.False;
            ultraGridColumn7.AllowRowSummaries = Infragistics.Win.UltraWinGrid.AllowRowSummaries.False;
            ultraGridColumn7.AutoCompleteMode = Infragistics.Win.AutoCompleteMode.None;
            ultraGridColumn7.CellActivation = Infragistics.Win.UltraWinGrid.Activation.ActivateOnly;
            ultraGridColumn7.DefaultCellValue = "Log Event Dialog";
            ultraGridColumn7.Header.VisiblePosition = 7;
            ultraGridColumn7.Style = Infragistics.Win.UltraWinGrid.ColumnStyle.Button;
            ultraGridColumn7.Width = 117;
            ultraGridBand1.Columns.AddRange(new object[] {
            ultraGridColumn1,
            ultraGridColumn2,
            ultraGridColumn3,
            ultraGridColumn4,
            ultraGridColumn5,
            ultraGridColumn6,
            ultraGridColumn8,
            ultraGridColumn7});
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
            this.ultraGrid1.Location = new System.Drawing.Point(2, 200);
            this.ultraGrid1.Name = "ultraGrid1";
            this.ultraGrid1.Size = new System.Drawing.Size(1285, 413);
            this.ultraGrid1.TabIndex = 3;
            this.ultraGrid1.Text = "Result";
            this.ultraGrid1.InitializeLayout += new Infragistics.Win.UltraWinGrid.InitializeLayoutEventHandler(this.ultraGrid1_InitializeLayout);
            this.ultraGrid1.InitializeRow += new Infragistics.Win.UltraWinGrid.InitializeRowEventHandler(this.ultraGrid1_InitializeRow);
            this.ultraGrid1.ClickCellButton += new Infragistics.Win.UltraWinGrid.CellEventHandler(this.ultraGrid1_ClickCellButton);
            this.ultraGrid1.MouseDown += new System.Windows.Forms.MouseEventHandler(this.ultraGrid1_MouseDown);
            // 
            // ultraGroupBox1
            // 
            this.ultraGroupBox1.Controls.Add(this.ultraTextEditorCluster);
            this.ultraGroupBox1.Location = new System.Drawing.Point(218, 10);
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
            this.ultraGroupBox2.Location = new System.Drawing.Point(442, 10);
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
            this.ultraGroupBox4.Location = new System.Drawing.Point(23, 150);
            this.ultraGroupBox4.Name = "ultraGroupBox4";
            this.ultraGroupBox4.Size = new System.Drawing.Size(625, 44);
            this.ultraGroupBox4.TabIndex = 7;
            this.ultraGroupBox4.Text = "Process Mapper JSON File or string";
            // 
            // ultraCheckEditorDisableParallelProcessing
            // 
            this.ultraCheckEditorDisableParallelProcessing.Location = new System.Drawing.Point(118, 100);
            this.ultraCheckEditorDisableParallelProcessing.Name = "ultraCheckEditorDisableParallelProcessing";
            this.ultraCheckEditorDisableParallelProcessing.Size = new System.Drawing.Size(108, 48);
            this.ultraCheckEditorDisableParallelProcessing.TabIndex = 8;
            this.ultraCheckEditorDisableParallelProcessing.Text = "Disable Parallel Processing";
            // 
            // ultraStatusBar1
            // 
            this.ultraStatusBar1.Location = new System.Drawing.Point(0, 619);
            this.ultraStatusBar1.Name = "ultraStatusBar1";
            ultraStatusPanel1.Key = "Operations";
            ultraStatusPanel1.SizingMode = Infragistics.Win.UltraWinStatusBar.PanelSizingMode.Automatic;
            ultraStatusPanel1.ToolTipText = "Current Operations";
            ultraStatusPanel1.Width = 15;
            ultraStatusPanel1.WrapText = Infragistics.Win.DefaultableBoolean.False;
            ultraStatusPanel2.Key = "Progress";
            ultraStatusPanel2.MinWidth = 100;
            ultraStatusPanel2.SizingMode = Infragistics.Win.UltraWinStatusBar.PanelSizingMode.Spring;
            ultraStatusPanel2.Style = Infragistics.Win.UltraWinStatusBar.PanelStyle.AutoStatusText;
            ultraStatusPanel2.Width = 300;
            ultraStatusPanel3.Key = "LoggingStatus";
            ultraStatusPanel3.SizingMode = Infragistics.Win.UltraWinStatusBar.PanelSizingMode.Automatic;
            ultraStatusPanel3.ToolTipText = "Logging Status";
            this.ultraStatusBar1.Panels.AddRange(new Infragistics.Win.UltraWinStatusBar.UltraStatusPanel[] {
            ultraStatusPanel1,
            ultraStatusPanel2,
            ultraStatusPanel3});
            this.ultraStatusBar1.Size = new System.Drawing.Size(1296, 25);
            this.ultraStatusBar1.TabIndex = 9;
            this.ultraStatusBar1.Text = "ultraStatusBar1";
            // 
            // timerProgress
            // 
            this.timerProgress.Enabled = true;
            this.timerProgress.Interval = 2000;
            this.timerProgress.Tick += new System.EventHandler(this.timerProgress_Tick);
            // 
            // buttonSyncExcute
            // 
            this.buttonSyncExcute.Location = new System.Drawing.Point(23, 51);
            this.buttonSyncExcute.Name = "buttonSyncExcute";
            this.buttonSyncExcute.Size = new System.Drawing.Size(75, 23);
            this.buttonSyncExcute.TabIndex = 10;
            this.buttonSyncExcute.Text = "Sync-Execute";
            this.buttonSyncExcute.UseVisualStyleBackColor = true;
            this.buttonSyncExcute.Click += new System.EventHandler(this.buttonSyncExcute_Click);
            // 
            // buttonClearResults
            // 
            this.buttonClearResults.Location = new System.Drawing.Point(118, 51);
            this.buttonClearResults.Name = "buttonClearResults";
            this.buttonClearResults.Size = new System.Drawing.Size(75, 23);
            this.buttonClearResults.TabIndex = 11;
            this.buttonClearResults.Text = "Clear";
            this.buttonClearResults.UseVisualStyleBackColor = true;
            this.buttonClearResults.Click += new System.EventHandler(this.buttonClearResults_Click);
            // 
            // buttonOpenLogFile
            // 
            this.buttonOpenLogFile.Location = new System.Drawing.Point(23, 93);
            this.buttonOpenLogFile.Name = "buttonOpenLogFile";
            this.buttonOpenLogFile.Size = new System.Drawing.Size(75, 23);
            this.buttonOpenLogFile.TabIndex = 12;
            this.buttonOpenLogFile.Text = "Open Log";
            this.buttonOpenLogFile.UseVisualStyleBackColor = true;
            this.buttonOpenLogFile.Click += new System.EventHandler(this.buttonOpenLogFile_Click);
            // 
            // ultraGroupBox5
            // 
            this.ultraGroupBox5.Controls.Add(this.ultraDateTimeEditorStartLog);
            this.ultraGroupBox5.Location = new System.Drawing.Point(215, 56);
            this.ultraGroupBox5.Name = "ultraGroupBox5";
            this.ultraGroupBox5.Size = new System.Drawing.Size(200, 44);
            this.ultraGroupBox5.TabIndex = 5;
            this.ultraGroupBox5.Text = "Log Beginning Timeframe";
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
            // ultraGroupBox6
            // 
            this.ultraGroupBox6.Controls.Add(this.ultraDateTimeEditorEndLog);
            this.ultraGroupBox6.Location = new System.Drawing.Point(439, 56);
            this.ultraGroupBox6.Name = "ultraGroupBox6";
            this.ultraGroupBox6.Size = new System.Drawing.Size(200, 44);
            this.ultraGroupBox6.TabIndex = 6;
            this.ultraGroupBox6.Text = "Log Ending Timeframe";
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
            // ultraGroupBox7
            // 
            this.ultraGroupBox7.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraGroupBox7.Controls.Add(this.ultraTextEditorAdditionalLogs);
            this.ultraGroupBox7.Location = new System.Drawing.Point(658, 55);
            this.ultraGroupBox7.Name = "ultraGroupBox7";
            this.ultraGroupBox7.Size = new System.Drawing.Size(623, 44);
            this.ultraGroupBox7.TabIndex = 7;
            this.ultraGroupBox7.Text = "Additional Log Files";
            // 
            // ultraTextEditorAdditionalLogs
            // 
            editorButton4.Key = "FileEditor";
            editorButton4.Text = "...";
            this.ultraTextEditorAdditionalLogs.ButtonsRight.Add(editorButton4);
            this.ultraTextEditorAdditionalLogs.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorAdditionalLogs.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorAdditionalLogs.Multiline = true;
            this.ultraTextEditorAdditionalLogs.Name = "ultraTextEditorAdditionalLogs";
            this.ultraTextEditorAdditionalLogs.ShowOverflowIndicator = true;
            this.ultraTextEditorAdditionalLogs.Size = new System.Drawing.Size(617, 25);
            this.ultraTextEditorAdditionalLogs.TabIndex = 0;
            this.ultraTextEditorAdditionalLogs.WordWrap = false;
            this.ultraTextEditorAdditionalLogs.EditorButtonClick += new Infragistics.Win.UltraWinEditors.EditorButtonEventHandler(this.ultraTextEditorAdditionalLogs_EditorButtonClick);
            // 
            // ultraGroupBox8
            // 
            this.ultraGroupBox8.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraGroupBox8.Controls.Add(this.ultraTextEditorAdditionalDDLFiles);
            this.ultraGroupBox8.Location = new System.Drawing.Point(661, 100);
            this.ultraGroupBox8.Name = "ultraGroupBox8";
            this.ultraGroupBox8.Size = new System.Drawing.Size(623, 44);
            this.ultraGroupBox8.TabIndex = 7;
            this.ultraGroupBox8.Text = "Additional DDL Files";
            // 
            // ultraTextEditorAdditionalDDLFiles
            // 
            editorButton5.Key = "FileEditor";
            editorButton5.Text = "...";
            this.ultraTextEditorAdditionalDDLFiles.ButtonsRight.Add(editorButton5);
            this.ultraTextEditorAdditionalDDLFiles.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorAdditionalDDLFiles.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorAdditionalDDLFiles.Multiline = true;
            this.ultraTextEditorAdditionalDDLFiles.Name = "ultraTextEditorAdditionalDDLFiles";
            this.ultraTextEditorAdditionalDDLFiles.ShowOverflowIndicator = true;
            this.ultraTextEditorAdditionalDDLFiles.Size = new System.Drawing.Size(617, 25);
            this.ultraTextEditorAdditionalDDLFiles.TabIndex = 0;
            this.ultraTextEditorAdditionalDDLFiles.WordWrap = false;
            this.ultraTextEditorAdditionalDDLFiles.EditorButtonClick += new Infragistics.Win.UltraWinEditors.EditorButtonEventHandler(this.ultraTextEditorAdditionalDDLFiles_EditorButtonClick);
            // 
            // ultraGroupBox9
            // 
            this.ultraGroupBox9.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ultraGroupBox9.Controls.Add(this.ultraTextEditorAltCompFiles);
            this.ultraGroupBox9.Location = new System.Drawing.Point(663, 150);
            this.ultraGroupBox9.Name = "ultraGroupBox9";
            this.ultraGroupBox9.Size = new System.Drawing.Size(623, 44);
            this.ultraGroupBox9.TabIndex = 13;
            this.ultraGroupBox9.Text = "Additional Compression Files";
            // 
            // ultraTextEditorAltCompFiles
            // 
            editorButton6.Key = "FileEditor";
            editorButton6.Text = "...";
            this.ultraTextEditorAltCompFiles.ButtonsRight.Add(editorButton6);
            this.ultraTextEditorAltCompFiles.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ultraTextEditorAltCompFiles.Location = new System.Drawing.Point(3, 16);
            this.ultraTextEditorAltCompFiles.Multiline = true;
            this.ultraTextEditorAltCompFiles.Name = "ultraTextEditorAltCompFiles";
            this.ultraTextEditorAltCompFiles.ShowOverflowIndicator = true;
            this.ultraTextEditorAltCompFiles.Size = new System.Drawing.Size(617, 25);
            this.ultraTextEditorAltCompFiles.TabIndex = 0;
            this.ultraTextEditorAltCompFiles.WordWrap = false;
            this.ultraTextEditorAltCompFiles.EditorButtonClick += new Infragistics.Win.UltraWinEditors.EditorButtonEventHandler(this.ultraTextEditorAltCompFiles_EditorButtonClick);
            // 
            // ultraCheckEditorLogDisableMryMap
            // 
            this.ultraCheckEditorLogDisableMryMap.Location = new System.Drawing.Point(233, 100);
            this.ultraCheckEditorLogDisableMryMap.Name = "ultraCheckEditorLogDisableMryMap";
            this.ultraCheckEditorLogDisableMryMap.Size = new System.Drawing.Size(123, 48);
            this.ultraCheckEditorLogDisableMryMap.TabIndex = 14;
            this.ultraCheckEditorLogDisableMryMap.Text = "Disable Log Evt Memory Mapping";
            // 
            // ultraCheckEditorDisableSysDSEDDL
            // 
            this.ultraCheckEditorDisableSysDSEDDL.Location = new System.Drawing.Point(362, 100);
            this.ultraCheckEditorDisableSysDSEDDL.Name = "ultraCheckEditorDisableSysDSEDDL";
            this.ultraCheckEditorDisableSysDSEDDL.Size = new System.Drawing.Size(123, 48);
            this.ultraCheckEditorDisableSysDSEDDL.TabIndex = 15;
            this.ultraCheckEditorDisableSysDSEDDL.Text = "Disable System/DSE DDL Preloading";
            // 
            // FormMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1296, 644);
            this.Controls.Add(this.ultraCheckEditorDisableSysDSEDDL);
            this.Controls.Add(this.ultraCheckEditorLogDisableMryMap);
            this.Controls.Add(this.ultraGroupBox9);
            this.Controls.Add(this.ultraGroupBox8);
            this.Controls.Add(this.ultraGroupBox7);
            this.Controls.Add(this.ultraGroupBox6);
            this.Controls.Add(this.ultraGroupBox5);
            this.Controls.Add(this.buttonOpenLogFile);
            this.Controls.Add(this.buttonClearResults);
            this.Controls.Add(this.buttonSyncExcute);
            this.Controls.Add(this.ultraStatusBar1);
            this.Controls.Add(this.ultraCheckEditorDisableParallelProcessing);
            this.Controls.Add(this.ultraGroupBox4);
            this.Controls.Add(this.ultraGroupBox3);
            this.Controls.Add(this.ultraGroupBox2);
            this.Controls.Add(this.ultraGroupBox1);
            this.Controls.Add(this.ultraGrid1);
            this.Controls.Add(this.buttonCancelExecution);
            this.Controls.Add(this.buttonAsyncExcute);
            this.Name = "FormMain";
            this.Text = "DSE Diagnostic Parsing Engine";
            this.Load += new System.EventHandler(this.FormMain_Load);
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
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorStartLog)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox6)).EndInit();
            this.ultraGroupBox6.ResumeLayout(false);
            this.ultraGroupBox6.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraDateTimeEditorEndLog)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox7)).EndInit();
            this.ultraGroupBox7.ResumeLayout(false);
            this.ultraGroupBox7.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorAdditionalLogs)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox8)).EndInit();
            this.ultraGroupBox8.ResumeLayout(false);
            this.ultraGroupBox8.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorAdditionalDDLFiles)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraGroupBox9)).EndInit();
            this.ultraGroupBox9.ResumeLayout(false);
            this.ultraGroupBox9.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.ultraTextEditorAltCompFiles)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorLogDisableMryMap)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ultraCheckEditorDisableSysDSEDDL)).EndInit();
            this.ResumeLayout(false);

		}

        #endregion

        private System.Windows.Forms.Button buttonAsyncExcute;
        private System.Windows.Forms.Button buttonCancelExecution;
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
        private System.Windows.Forms.Button buttonSyncExcute;
        private System.Windows.Forms.Button buttonClearResults;
        private System.Windows.Forms.Button buttonOpenLogFile;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox5;
        private Infragistics.Win.UltraWinEditors.UltraDateTimeEditor ultraDateTimeEditorStartLog;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox6;
        private Infragistics.Win.UltraWinEditors.UltraDateTimeEditor ultraDateTimeEditorEndLog;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox7;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorAdditionalLogs;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox8;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorAdditionalDDLFiles;
        private Infragistics.Win.Misc.UltraGroupBox ultraGroupBox9;
        private Infragistics.Win.UltraWinEditors.UltraTextEditor ultraTextEditorAltCompFiles;
        private Infragistics.Win.UltraWinEditors.UltraCheckEditor ultraCheckEditorLogDisableMryMap;
        private Infragistics.Win.UltraWinEditors.UltraCheckEditor ultraCheckEditorDisableSysDSEDDL;
    }
}


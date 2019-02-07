VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "clsSheetEvent"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
Option Explicit

Public CollectinIdx As Integer
Public WithEvents Worksheet As Worksheet
Attribute Worksheet.VB_VarHelpID = -1
Private MenuDefined As Boolean

Private Sub Worksheet_BeforeRightClick(ByVal Target As Range, Cancel As Boolean)
    If Target.Rows.Count >= 1 And Target.row >= 2 And IsDate(Cells(Target.row, 14).value) Then
        MenuDefined = False
        BuildCustomMenuFndLogFile 'build new menu
        MenuDefined = True
    Else
    If MenuDefined Then
        DeleteCustomMenuFndLogFile 'remove possible duplicates
        MenuDefined = False
    End If
    End If
End Sub

Private Sub Worksheet_Deactivate()
  DeleteCustomMenuFndLogFile
  MenuDefined = False
End Sub

Private Sub Class_Terminate()
    DeleteCustomMenuFndLogFile
    MenuDefined = False
    Set Worksheet = Nothing
End Sub

Attribute VB_Name = "Module2"
Option Explicit

Sub BuildCustomMenuFndLogFile()
    Dim ContextMenu As CommandBar
    
    Call DeleteCustomMenuFndLogFile
    
    Set ContextMenu = Application.CommandBars("List Range Popup")
    
    With ContextMenu.Controls.Add(Temporary:=True, Before:=1)
        .OnAction = "'" & ThisWorkbook.Name & "'!" & "LogAggregation_FindLogFileFromLocalTS"
        .Style = msoButtonCaption
        .Caption = "Find Log File"
        .Tag = "FindLogFileFromLocalTS_Tag"
    End With

End Sub

Sub DeleteCustomMenuFndLogFile()

    Dim ContextMenu As CommandBar
    Dim ctrl As CommandBarControl

    ' Set ContextMenu to the Cell context menu.
    Set ContextMenu = Application.CommandBars("List Range Popup")

    ' Delete the custom controls with the Tag : My_Cell_Control_Tag.
    For Each ctrl In ContextMenu.Controls
        If ctrl.Tag = "FindLogFileFromLocalTS_Tag" Then
            ctrl.Delete
        End If
    Next ctrl
    
    
End Sub


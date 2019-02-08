VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "PivotItemInfo"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
Option Explicit
'Unrestricted class
Public Parent As PivotFieldrInfo
Public item As pivotItem
Public Name As String
Public IsBlank As Boolean
Public IsAll As Boolean

Public Property Get Selected() As Boolean

    If Me.IsAll Then
        Selected = True
    Else
        Selected = Me.item.Visible = True
    End If
    
End Property

Public Property Let Selected(value As Boolean)
    
    If Me.IsAll Then
        If value = True Then
            Call Me.Parent.SelectAll
        Else
            Call Me.Parent.SelectBlank
        End If
    ElseIf Not Me.item.Visible = value Then
        Me.item.Visible = value
        If Me.Parent.MultiSelection = False Then
            Me.Parent.Field.CurrentPage = IIf(value = True, Me.item.Caption, "(blank)")
        End If
    End If
    
End Property

Public Sub SetName(pItem As pivotItem)
    On Error Resume Next
    
    Dim sSetName As String
    
    sSetName = Trim(pItem.SourceName)
    
    If IsEmpty(sSetName) = True Then
        sSetName = Trim(pItem.Caption)
    End If
         
    Me.Name = sSetName
    
    On Error GoTo 0
    
End Sub

Public Function SetPivotItem(pItem As pivotItem, parentFilter As PivotFieldrInfo) As PivotItemInfo
    
    Set SetPivotItem = Me
    
    Set Me.Parent = parentFilter
    Set Me.item = pItem
    Call Me.SetName(pItem)
    Me.IsBlank = Me.Name = "(blank)"
    Me.IsAll = Me.Name = "(All)"
       
End Function
Public Function SetAsAllPivotItem(parentFilter As PivotFieldrInfo) As PivotItemInfo
    
    Set SetAsAllPivotItem = Me
    
    Set Me.Parent = parentFilter
    Set Me.item = Nothing
    Me.Name = "(All)"
    Me.IsBlank = False
    Me.IsAll = True
       
End Function


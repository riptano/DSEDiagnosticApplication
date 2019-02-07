VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "ProgressBar"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
' Class Module - ProgressBar
Option Explicit

Private statusBarState As Boolean
Private enableEventsState As Boolean
Private screenUpdatingState As Boolean
Private Const NUM_BARS As Integer = 50
Private Const MAX_LENGTH As Integer = 255
Private BAR_CHAR As String
Private SPACE_CHAR As String

Private Sub Class_Initialize()
    ' Save the state of the variables to change
    statusBarState = Application.DisplayStatusBar
    ' enableEventsState = Application.EnableEvents
    ' screenUpdatingState = Application.ScreenUpdating
    ' set the progress bar chars (should be equal size)
    BAR_CHAR = ChrW(9608)
    SPACE_CHAR = ChrW(9620)
    ' Set the desired state
    Application.DisplayStatusBar = True
    ' Application.ScreenUpdating = False
    ' Application.EnableEvents = False
End Sub

Private Sub Class_Terminate()
    ' Restore settings
    Application.DisplayStatusBar = statusBarState
    ' Application.ScreenUpdating = screenUpdatingState
    ' Application.EnableEvents = enableEventsState
    Application.StatusBar = False
End Sub

Public Sub update(ByVal value As Long, _
                  Optional ByVal MaxValue As Long = 0, _
                  Optional ByVal Status As String = "", _
                  Optional ByVal DisplayPercent As Boolean = True)

    ' Value          : 0 to 100 (if no max is set)
    ' Value          : >=0 (if max is set)
    ' MaxValue       : >= 0
    ' Status         : optional message to display for user
    ' DisplayPercent : Display the percent complete after the status bar

    ' <Status> <Progress Bar> <Percent Complete>

    ' Validate entries
    If value < 0 Or MaxValue < 0 Or (value > 100 And MaxValue = 0) Then Exit Sub

    ' If the maximum is set then adjust value to be in the range 0 to 100
    If MaxValue > 0 Then value = WorksheetFunction.RoundUp((value * 100) / MaxValue, 0)

    ' Message to set the status bar to
    Dim display As String
    display = Status & "  "

    ' Set bars
    display = display & String(Int(value / (100 / NUM_BARS)), BAR_CHAR)
    ' set spaces
    display = display & String(NUM_BARS - Int(value / (100 / NUM_BARS)), SPACE_CHAR)

    ' Closing character to show end of the bar
    display = display & BAR_CHAR

    If DisplayPercent = True Then display = display & "  (" & value & "%)  "

    ' chop off to the maximum length if necessary
    If Len(display) > MAX_LENGTH Then display = Right(display, MAX_LENGTH)

    Application.StatusBar = display
End Sub


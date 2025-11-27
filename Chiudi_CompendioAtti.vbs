Option Explicit

Dim fso, shell, currentDir, batPath

Set fso   = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

' Cartella in cui si trova questo VBS
currentDir = fso.GetParentFolderName(WScript.ScriptFullName)

' Percorso completo del BAT
batPath = fso.BuildPath(currentDir, "Chiudi_CompendioAtti.bat")

' Eseguo il BAT nascosto, senza aspettare
shell.Run """" & batPath & """", 0, False

Set WshShell = CreateObject("WScript.Shell")

' Avvia Python embedded in modalità invisibile (flag 0)
WshShell.Run "python_embed\python.exe CompendioAtti.py", 0

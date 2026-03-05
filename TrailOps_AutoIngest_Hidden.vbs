' TrailOps - Auto ingest hidden launcher (no console window)
' Runs the existing TrailOps_AutoIngest.bat invisibly.
' Place in: C:\trail_ops\TrailOps_AutoIngest_Hidden.vbs

Option Explicit

Dim shell, cmd
Set shell = CreateObject("WScript.Shell")

cmd = "cmd.exe /c ""C:\trail_ops\TrailOps_AutoIngest.bat"""
' 0 = hidden window, False = don't wait
shell.Run cmd, 0, False

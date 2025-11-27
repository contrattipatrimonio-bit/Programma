@echo off
setlocal

set NETDIR=\\dp-smb\Supporto_Coordinamento\Compendio
set LOCALDIR=%~dp0

echo *** Controllo aggiornamenti... ***

for %%F in (CompendioAtti.py Avvia_CompendioAtti.bat) do (
    if exist "%NETDIR%\%%F" (
        for %%A in ("%NETDIR%\%%F") do set NETDATE=%%~tA
        for %%B in ("%LOCALDIR%\%%F") do set LOCDATE=%%~tB

        if "!NETDATE!" GTR "!LOCDATE!" (
            echo Aggiorno %%F ...
            copy /Y "%NETDIR%\%%F" "%LOCALDIR%"
        )
    )
)

echo Aggiornamento completato.
timeout /t 1 >nul

endlocal
exit

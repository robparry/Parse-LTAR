@echo off
rem e:\Anaconda\Scripts\ipython C:\Users\rparry\Documents\PythonScripts\LTARdata\parse_CR1000.py %*
cd C:\Users\rparry\Documents\PythonScripts\LTARdata

rem e:\Anaconda\python C:\Users\rparry\Documents\PythonScripts\LTARdata\parse_CR1000.py %*
rem April 27, 2017 - changed to parse both station 2 and station 3
e:\Anaconda\python C:\Users\rparry\Documents\PythonScripts\LTARdata\parse_CR1000.py -f Sta3LTAR15min.dat -d \\10.18.26.36\Meyers
e:\Anaconda\python C:\Users\rparry\Documents\PythonScripts\LTARdata\parse_CR1000.py -f Sta2LTAR15min.dat -d \\10.18.26.36\Meyers

rem edit the FTP transfer instructions to add the "cd ltar-barc" statement at beginning and "quit" at end
rem created this way to be able to have multiple stations transferred at once
rem this also has the added benefit that when no data has been uploaded from the stations (so no files have been changed)
rem that the ftp-commands.txt file will contain only those 2 lines, so no files are transferred
echo cd ltar-barc > ftp-commands.txt
type transfer_to_FTP_server.txt >> ftp-commands.txt
echo quit >> ftp-commands.txt

rem now use psftp to transfer the CSV file to ARS FTP site
echo FTP transfer of CSV data files and MD5 hash ...
"C:\Program Files (x86)\psftp" -l sv.03.mdnal.barc@usda.gov -pw Ba123456123456 -b ftp-commands.txt ftp.arsnet.usda.gov

rem when done, remove the FTP transfer instructions so they can be built from scratch next time.
del transfer_to_FTP_server.txt
move ftp-commands.txt ftp-commands-save.txt

chcp 65001

@echo off
set root=C:\Users\6312lab\anaconda3

call %root%\Scripts\activate.bat %root%

rem call conda list pandas

"C:\Users\6312lab\Anaconda3\python.exe" "C:\Users\6312lab\Desktop\Lisa\case\download_csvfile.py"

pause
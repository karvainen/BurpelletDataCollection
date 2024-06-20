@echo off

if exist "D:\Python\Napit.py" (
    start cmd /k python "D:\Python\Napit.py"
) else (
    echo File ParmetriNodet.py not found!
    pause
)

if exist "D:\Python\Data.py" (
    start cmd /k python "D:\Python\Data.py"
) else (
    echo File DataNodet.py not found!
    pause
)

if exist "D:\Python\Parametrit.py" (
    start cmd /k python "D:\Python\Parametrit.py"
) else (
    echo File ButtonNodet.py not found!
    pause
)

pause
@echo off

if exist "D:\Python\ButtonSave.py" (
    start cmd /k python "D:\Python\ButtonSave.py"
) else (
    echo File ParmetriNodet.py not found!
    pause
)

if exist "D:\Python\ParameterSave.py" (
    start cmd /k python "D:\Python\ParameterSave.py"
) else (
    echo File DataNodet.py not found!
    pause
)

if exist "D:\Python\DataSave.py" (
    start cmd /k python "D:\Python\DataSave.py"
) else (
    echo File ButtonNodet.py not found!
    pause
)

pause

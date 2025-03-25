@echo off
setlocal ENABLEDELAYEDEXPANSION

REM Define the input file name and chunk size
set "filename=merged_content_20250307_114432.txt"
set "chunk_size=209715200"  REM 200MB in bytes

REM Check if the file exists
if not exist "%filename%" (
    echo File "%filename%" not found!
    pause
    exit /b
)

REM Initialize variables
set "chunk_count=0"
set "bytes_read=0"

REM Create a temporary file for storing chunks
set "tempfile=%filename%.part"

REM Read the file line by line
for /F "delims=" %%A in ('type "%filename%"') do (
    set "line=%%A"
    set /A bytes_read+=1

    REM Write the line to the temporary chunk file
    echo !line! >> "%tempfile%!chunk_count!.txt"

    REM Check if the chunk size limit is reached
    if !bytes_read! GEQ %chunk_size% (
        set /A chunk_count+=1
        set "bytes_read=0"
    )
)

echo Splitting complete. Created !chunk_count! files.
pause
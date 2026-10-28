Set-Location "C:\Users\thati\OneDrive\Desktop\영준\CURSOR\blog-challenge\scraper"
$log = "C:\Users\thati\OneDrive\Desktop\영준\CURSOR\blog-challenge\scraper\collect_log.txt"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content $log "[$timestamp] 수집 시작"
& "C:\Users\thati\AppData\Local\Programs\Python\Python314\python.exe" collect.py 2>&1 | Add-Content $log
$timestamp2 = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content $log "[$timestamp2] 수집 완료`n"

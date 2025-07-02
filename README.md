# trade_check

Objective => 
Check the dropcopy file and the trade files from file downloader to see weather all the entiries are matching or not. Also check for any mismatches between the dropcopy and the Qi trade's api.

How to run =>
1. Make 2 folder in the main directory named Our_file and Their_file.
2. Paste the trade file downloaded from file downloader in Our_file folder. **Filename format - net_positions_backup_20250702**
3. download dropcopy trade files using mobxterm and winscp.
4. Paste the dropcopy files in the Their_file folder. **Filename format - dropcopy_main_positions_20241108_161951**
5. Run the batch file - start_check.bat to check the mismatch between the files from Our_file and Their_file folder.
6. Run the batch file - api_check.bat to check the mismatch between the files from Their_file and the Qi trade's API folder.

1. Open BB analysis template Excel file(bb_analysis_template.xlsx).
2. In Sheet "RAW DATA I", clean all data and click Cell "A1".
3. Click "Data -> Get External Data -> From Text", select the first times.csv(3 rounds in one file), and click "Import".
4. In "Text Import Wizard", choose the file type "Delimited", check "My data has headers", and click "Next".
5. Check Delimiters "Other", input "|", and click "Next".
6. No change in data format, click "Finish".
7. In "Import Data" dialog, keep default Existing worksheet with "=$A$1" (set in step #2), click "OK"
8. Repeat step #2-#7 to import the second csv to Sheet "RAW DATA II"
9. See comparison results in Sheet "COMPARISON - XXX". "- ALL" is comparison of 3-round average, "- ROUND #X" is comparison of round X
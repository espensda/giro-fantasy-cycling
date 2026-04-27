# Excel Import Guide

## Overview
The Admin panel now supports Excel file uploads (`.xlsx`) for bulk importing stage points.

## Stage Results CSV Format (Optional)

**Required columns:** `position`, `name`, `team`
**Optional columns:** `time`

Example:
```
position,name,team,time
1,Ayuso Juan,UAE Team Emirates - XRG (WT),0:00
2,Del Toro Isaac,UAE Team Emirates - XRG (WT),+0:12
3,Pogacar Tadej,UAE Team Emirates - XRG (WT),+0:15
```

**Notes:**
- `position`: Integer (1, 2, 3, etc.)
- `name`: Must exactly match rider name in database
- `team`: Team name (used for reference)
- `time`: Optional (gap from first place, e.g., "0:00", "+0:12")

## Stage Points Excel Format (Single File)

**Required columns:** `name`, `points`

Example:
```
name,points
Pogacar Tadej,28
Ayuso Juan,23
Landa Mikel,16
```

**Notes:**
- `name`: Must exactly match rider name in database
- `points`: Total points for that stage.
- This total points value includes GC, mountain, and sprint points for the stage.

## Upload Workflow

1. **Select Stage**: Choose the stage number in the Admin panel
2. **Upload Excel**: Click file uploader and select your `.xlsx` file
3. **Preview**: See a preview of the data
4. **Validate**: Check for any unmatched rider names
5. **Import**: Click "Import" button to save to database

## Matching Rider Names

Rider names must **exactly** match those in the database. You can:
- View the database contents in Admin → View Database
- Copy exact names from the database to your Excel file
- Use the "Saved Stage Results" section to check imported names

## Tips

- Use a spreadsheet app (Excel, Google Sheets) to create/edit files
- Export as Excel format (`.xlsx`)
- Upload one stage points Excel file per stage with `name,points`
- Uploading will replace any previously saved data for that stage

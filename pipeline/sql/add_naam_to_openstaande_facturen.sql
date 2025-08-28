-- =====================================================================
-- Add Naam column to OpenstaandeFacturen table
-- This column will store the businessUser.fullName from the API
-- =====================================================================

USE tree11;
GO

-- Check if column already exists
IF NOT EXISTS (
    SELECT * FROM sys.columns 
    WHERE object_id = OBJECT_ID('tree11.OpenstaandeFacturen') 
    AND name = 'Naam'
)
BEGIN
    -- Add Naam column
    ALTER TABLE tree11.OpenstaandeFacturen 
    ADD Naam NVARCHAR(255) NULL;
    
    PRINT 'Kolom Naam toegevoegd aan tree11.OpenstaandeFacturen';
    
    -- Add comment to document the column
    EXEC sp_addextendedproperty 
        @name = N'MS_Description',
        @value = N'Volledige naam van de klant (businessUser.fullName)',
        @level0type = N'SCHEMA',
        @level0name = N'tree11',
        @level1type = N'TABLE',
        @level1name = N'OpenstaandeFacturen',
        @level2type = N'COLUMN',
        @level2name = N'Naam';
        
    PRINT 'Comment toegevoegd aan kolom Naam';
END
ELSE
BEGIN
    PRINT 'Kolom Naam bestaat al in tree11.OpenstaandeFacturen';
END
GO

-- Verify the column was added
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'tree11' 
AND TABLE_NAME = 'OpenstaandeFacturen'
AND COLUMN_NAME = 'Naam';
GO

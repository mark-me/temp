In DA_Central_Staging staan Scalars met meerdere variabelen. Ik weet niet of Genesis daar goed mee om gaat.

bv. @IsValid = CASE WHEN (@MaxIngDat IS NOT NULL AND @LDGDat >= CAST(GETDATE() AS DATE)) THEN 1 ELSE 0 END
CREATE FUNCTION [DA_Central].[fn_CheckDateIsEasterDate] (@date [date]) RETURNS bit
AS
BEGIN;
    --- Variables used:
    DECLARE @a tinyint, @b tinyint, @c tinyint,
            @d tinyint, @e tinyint, @f tinyint,
            @g tinyint, @h tinyint, @i tinyint,
            @k tinyint, @l tinyint, @m tinyint,
            @easterdate date, @iseasterdate bit, 
			@year int ;

    --- Calculation steps:
	SET @year=YEAR(@date)
    SET @a=@year%19
	SET @b=FLOOR(1.0*@year/100)
	SET @c=@year%100;
    SET @d=FLOOR(1.0*@b/4)
	SET @e=@b%4
	SET @f=FLOOR((8.0+@b)/25);
    SET @g=FLOOR((1.0+@b-@f)/3);
    SET @h=(19*@a+@b-@d-@g+15)%30
	SET @i=FLOOR(1.0*@c/4)
	SET @k=@year%4;
    SET @l=(32.0+2*@e+2*@i-@h-@k)%7;
    SET @m=FLOOR((1.0*@a+11*@h+22*@l)/451);
    SET @easterdate=
           CAST(
		   DATEADD(dd, (@h+@l-7*@m+114)%31,
            DATEADD(mm, FLOOR((1.0*@h+@l-7*@m+114)/31)-1,
                DATEADD(yy, @year-2000, {d '2000-01-01'})
            )
        ) AS DATE)
	SET @iseasterdate = CASE WHEN @date = @easterdate THEN 1 ELSE 0 END
    ----- Return the 1 if date is first easter day:
    RETURN @iseasterdate;

END
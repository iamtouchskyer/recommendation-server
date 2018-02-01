CREATE PROCEDURE [dbo].[prc_playCountByHour]
	@videoType varchar(10) = null,
	@provinceID int = -1,
	@startDate DATE,
	@endDate DATE
AS
	SELECT DATEADD(hour, datediff(hour, 0, DATEADD(hour, 8, servertime)), 0) AS     PlayHour,
    COUNT(*) AS PlayCount
  FROM events
  WHERE cmdid = '13'
    AND (videoType = @videoType OR @videoType IS NULL)

    AND CONVERT(DATE, DATEADD(hour, datediff(hour, 0, DATEADD(hour, 8, servertime)), 0)) >= @startDate
    AND CONVERT(DATE, DATEADD(hour, datediff(hour, 0, DATEADD(hour, 8, servertime)), 0)) <= @endDate

    AND (provinceid = @provinceID OR @provinceID = -1)

  GROUP BY DATEADD(hour, datediff(hour, 0, DATEADD(hour, 8, servertime)), 0)
  ORDER BY DATEADD(hour, datediff(hour, 0, DATEADD(hour, 8, servertime)), 0)
RETURN
CREATE PROCEDURE [dbo].[prc_rankVideoByPlayCount]
	@videoType varchar(10),
	@language nvarchar(10) = null,
	@category nvarchar(20) = null,
	@area nvarchar(20) = null,
	@provinceID int = -1,
	@startDate DATE,
	@endDate DATE,
	@hourOfDay int = 25, -- 25 is not gonna happen...
	@top int = 10
AS
	WITH FilteredVideo (vid, vname) 
	AS
	(
			SELECT [vid], [vname]
			FROM videoinfo 
			WHERE [videotype] = @videoType 
				AND ([language] LIKE N'%' + @language + '%' OR @language IS NULL)
				AND ([category] LIKE N'%' + @category + '%' OR @category IS NULL)
				AND ([area] LIKE N'%' + @area + '%' OR @area IS NULL)
	)
	SELECT TOP(@top) events.[vid], FilteredVideo.vname AS videoname, COUNT(events.vid) as play_count
	FROM dbo.events RIGHT JOIN FilteredVideo ON (events.vid = FilteredVideo.vid)
	WHERE cmdid='13' 
		AND CONVERT(DATE, servertime) >= @startDate
		AND CONVERT(DATE, servertime) <= @endDate
		AND videotype = @videoType			-- VIDEO TYPE
		AND (DATEPART(HOUR, servertime) = @hourOfDay OR @hourOfDay = 25)	-- HOUR OF DAY
		AND (provinceid = @provinceID OR @provinceID = -1)			-- PROVINCE 
	GROUP BY events.[vid], FilteredVideo.vname
	ORDER BY play_count DESC
RETURN
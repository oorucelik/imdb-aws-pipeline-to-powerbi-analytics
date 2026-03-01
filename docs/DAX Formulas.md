# DAX Formulas — Measures & Calculated Columns

All DAX formulas used in the Power BI report, extracted from the Tabular Editor data model.

---

## Key Measures

Centralized in the `Key Measures` table (no data — measures only).

### Time Intelligence

#### Latest Popularity Date
> Returns the most recent load date across all fact data, ignoring any content filter context.
```dax
CALCULATE(
    MAX(fact_popularity[loaddate]),
    ALL(fact_popularity)
)
```

#### Popularity Today
> Today's popularity score for the selected content.
```dax
VAR latestDate = [Latest Popularity Date]
RETURN
CALCULATE(
    SUM(fact_popularity[Popularity]),
    fact_popularity[loaddate]=latestDate
)
```

#### Content Popularity Yesterday
> Yesterday's popularity score, used for comparisons and trend visuals.
```dax
VAR latestDate =
 CALCULATE(
    MAX(fact_popularity[loaddate]),
    ALL(fact_popularity)
)
VAR yesterdayDate = 
CALCULATE(
    MAX(fact_popularity[loaddate]),
    fact_popularity[loaddate]<latestDate
)

RETURN 
CALCULATE(
    MAX(fact_popularity[Popularity]),
    fact_popularity[loaddate]= yesterdayDate,
    ALLEXCEPT(dim_content,dim_content[content_id],dim_content[Title])   
)
```

#### Rank Today
> Overall popularity rank for today (across all content types).
```dax
VAR latestDate = [Latest Popularity Date]
RETURN
    CALCULATE(
        SUM(fact_popularity[Rank]),
        fact_popularity[loaddate]=latestDate
    )
```

#### Rank Change Today
> Change in overall rank vs previous day.
```dax
VAR latestDate = [Latest Popularity Date]
RETURN
    CALCULATE(
        SUM(fact_popularity[Rank Change]),
        fact_popularity[loaddate]=latestDate
    )
```

#### Rank TypeWise Today
> Rank within the content's type (Movie or TV-Series). Uses `TREATAS` to maintain type filter across tables.
```dax
VAR LatestDate = [Latest Popularity Date]
VAR CurrentContentID = SELECTEDVALUE(dim_content[content_id])
VAR CurrentType = SELECTEDVALUE(dim_content[TypeDisplay])
VAR CurrentPopularity = 
    CALCULATE(
        VALUES(fact_popularity[popularity]),
        fact_popularity[loaddate] = LatestDate
    )

RETURN
IF(
    NOT ISBLANK(CurrentContentID),
    1 + CALCULATE(
            COUNTROWS(fact_popularity),
            fact_popularity[loaddate] = LatestDate,
            fact_popularity[popularity] > CurrentPopularity,
            TREATAS(VALUES(dim_content[TypeDisplay]), dim_content[TypeDisplay]),
            ALL(dim_content),
            ALL(fact_popularity)
    ),
    BLANK()
)
```

#### Rank TypeWise Yesterday
> Same as Rank TypeWise Today, but calculated for the previous day (used in rank change comparison).
```dax
VAR LatestDate = [Latest Popularity Date]
VAR YesterdayDate = CALCULATE(
                        MAX(fact_popularity[loaddate]),
                        fact_popularity[loaddate]<LatestDate)
VAR CurrentContentID = SELECTEDVALUE(dim_content[content_id])
VAR CurrentType = SELECTEDVALUE(dim_content[TypeDisplay])
VAR CurrentPopularity = 
    CALCULATE(
        VALUES(fact_popularity[popularity]),
        fact_popularity[loaddate] = YesterdayDate
    )

RETURN
IF(
    NOT ISBLANK(CurrentContentID),
    1 + CALCULATE(
        COUNTROWS(fact_popularity),
        fact_popularity[loaddate] = YesterdayDate,
        fact_popularity[popularity] > CurrentPopularity,
        TREATAS(VALUES(dim_content[TypeDisplay]), dim_content[TypeDisplay]),
        ALL(dim_content),
        ALL(fact_popularity)
    ),
    BLANK()
)
```

#### Rank Change TypeWise Today
> Difference between today's and yesterday's type-wise rank.
```dax
VAR todayRank = [Rank TypeWise Today]
VAR yesterdayRank = [Rank TypeWise Yesterday]
RETURN todayRank - yesterdayRank
```

#### Max Popularity Today
> Highest popularity score today (across all content, ignores dim_content filter).
```dax
VAR latestDate = [Latest Popularity Date]
RETURN 
CALCULATE(
    MAX(fact_popularity[Popularity]),
    fact_popularity[loaddate]= latestDate,
    REMOVEFILTERS(dim_content)   
)
```

#### Min Popularity Today
> Lowest popularity score today (excluding leavers).
```dax
VAR latestDate = [Latest Popularity Date]
RETURN 
CALCULATE(
    MIN(fact_popularity[Popularity]),
    fact_popularity[loaddate]= latestDate,
    REMOVEFILTERS(dim_content),
    fact_popularity[is_leaver]=FALSE
)
```

---

### KPIs

#### Quick Watch#
> Count of movies with runtime ≤ 90 min and rating ≥ 7.
```dax
VAR val = CALCULATE(
    DISTINCTCOUNT(dim_content[content_id]),
    dim_content[Runtime (Min.)] <= 90,
    NOT(ISBLANK(dim_content[Runtime (Min.)])),
    dim_content[Score] >= 7
)
RETURN IF(ISBLANK(val),0,val)
```

#### Binge Worthy#
> Count of TV series with avg episode runtime ≤ 45 min, rating ≥ 7, and total runtime < 480 min.
```dax
VAR val =
CALCULATE(
    COUNTROWS(
        FILTER(
            VALUES(dim_content[content_id]),
            VAR totalRuntime = CALCULATE(SUM(dim_content[TotalRuntime_Series]))
            VAR ContentScore = CALCULATE(SUM(dim_content[Score]))
            VAR avgEpisodeRuntime = CALCULATE(SUM(dim_content[AVG_Episode_Runtime]))
            RETURN
            avgEpisodeRuntime>0 && avgEpisodeRuntime <=45 && ContentScore >= 7 && totalRuntime > 0 && totalRuntime <=480
        )
    )
)
RETURN IF(ISBLANK(val),0,val)
```

#### Hidden Gems Rank
> Ranks content with rating ≥ 7 by total votes (ascending) — least-voted high-rated content ranks first.
```dax
VAR _Score = SELECTEDVALUE(dim_content[Score])
RETURN
IF(
    _Score >= 7,
    RANKX(
        FILTER(
            ALL(dim_content),
            dim_content[Score] >= 7
        ),
        CALCULATE(SUM(dim_content[Total Vote])),,ASC,DENSE
    ),
    BLANK()
)
```

#### Global Reach#
> Count of movies with revenue ≥ $500M.
```dax
VAR val = CALCULATE(
    DISTINCTCOUNT(dim_content[content_id]),
    dim_content[Revenue]>=500000000
)
RETURN IF(ISBLANK(val),0,val)
```

#### Oncoming#
> Count of content not yet released (release date > latest popularity date).
```dax
VAR latestDate = [Latest Popularity Date]
VAR val = CALCULATE(
    DISTINCTCOUNT(dim_content[content_id]),
    dim_content[Release Date]> latestDate
)
RETURN IF(ISBLANK(val),0,val)
```

#### Contents Raise#
> Count of content that improved in rank today (rank change < 0).
```dax
VAR latestDate = [Latest Popularity Date]
RETURN
CALCULATE(
    COUNTROWS(dim_content),
    fact_popularity[loaddate] = latestDate,
    fact_popularity[Rank Change] < 0  
)
```

#### ROI
> Return on Investment: (Revenue – Budget) / Budget.
```dax
DIVIDE(SUM(dim_content[Revenue])-SUM(dim_content[Budget]),SUM(dim_content[Budget]))
```
*Format: `0.00`*

#### ROI%
> Same as ROI but formatted as percentage.
```dax
DIVIDE(SUM(dim_content[Revenue])-SUM(dim_content[Budget]),SUM(dim_content[Budget]))
```
*Format: `%0.0`*

---

### Aggregations

#### Total Content#
```dax
DISTINCTCOUNT(dim_content[content_id])
```

#### Total Vote
> Sum of votes with dynamic K/M formatting.
```dax
SUM(dim_content[Total Vote])
```
*Dynamic format string:*
```dax
SWITCH(TRUE(), totalVote<1000,"0", totalVote<1000000,"#,.0K", totalVote<1000000000,"#,,.0M")
```

#### Total Budget
```dax
SUM(dim_content[Budget])
```
*Dynamic format string:*
```dax
SWITCH(TRUE(), totalBudget<1000,"$0", totalBudget<1000000,"$#,.0K", totalBudget<1000000000,"$#,,.0M", totalBudget>1000000000,"$#,,,.0B")
```

#### Total Revenue
```dax
SUM(dim_content[Revenue])
```
*Dynamic format: same pattern as Total Budget*

#### Total Runtime Series
```dax
SUM(dim_episode[Runtime (Min.)])
```

#### Avg Series Score
```dax
ROUND(DIVIDE(SUM(dim_episode[Score]),COUNT(dim_episode[Score])),1)
```

#### Network#
> Count of networks a content airs on.
```dax
COUNTX(
    RELATEDTABLE(bridge_content_network),
    bridge_content_network[network_id]
)
```

#### Prod. Company#
> Count of production companies for a content.
```dax
COUNTX(
    RELATEDTABLE(bridge_content_company),
    SELECTEDVALUE(bridge_content_company[company_id])
)
```

#### Total Vote Episode
```dax
SUM(dim_episode[Total Vote])
```
*Dynamic format: K/M*

---

### Display Measures

#### Rank Change Today Display
> Shows rank change with +/- prefix (inverted: negative rank_change = improvement = "+").
```dax
VAR rankChange = [Rank Change Today]
VAR absValue = ABS(rankChange)
VAR prefix = IF(rankChange<0,"+","-")
RETURN
    IF(
        rankChange<>0 && NOT(ISBLANK(rankChange)),
        prefix & absValue,
        ""
    )
```

#### Rank Change Typewise Today Display
> Same pattern as above, using `[Rank Change TypeWise Today]`.

#### Score Typewise Display
> Shows movie score or average series episode score depending on content type.
```dax
VAR scoreMovie = SUM(dim_content[Score])
VAR scoreSerie = [Avg Series Score]
VAR typeContent = MAX(dim_content[TypeDisplay])
RETURN 
IF(
    typeContent = "Movie",
    scoreMovie,
    scoreSerie
)
```

### Dates

#### Latest Popularity Date
*See Time Intelligence section above.*

#### Earliest Air Date
```dax
MIN(dim_episode[Air Date])
```
*Format: `dd/mm/yyyy`*

#### Latest Air Date
```dax
MAX(dim_episode[Air Date])
```
*Format: `dd/mm/yyyy`*

---

## Calculated Columns

### dim_content

#### TypeDisplay
> Converts raw `Type` column to friendly label.
```dax
VAR contentType = UPPER(dim_content[Type])
RETURN IF(CONTAINSSTRING(contentType,"MOVIE"),"Movie","TV-Series")
```

#### Genres
> Comma-separated list of genres from bridge table.
```dax
CONCATENATEX(
    RELATEDTABLE(bridge_content_genre),
    RELATED(dim_genre[Genres]),
    ", ")
```

#### Sub Genres
> Comma-separated list of interest tags from bridge table.
```dax
CONCATENATEX(
    RELATEDTABLE(bridge_content_interest),
    RELATED(dim_interest[Sub Genres]),
    ", "
)
```

#### Cast
> Top 3 actors/actresses by billing order.
```dax
CONCATENATEX(
    TOPN(
        3,
        FILTER(
            RELATEDTABLE(bridge_content_person),
            NOT(ISBLANK(bridge_content_person[order_no])) &&
            (bridge_content_person[role_type] = "Actor" || bridge_content_person[role_type] = "Actress")
        ),
        bridge_content_person[order_no], ASC
    ),
    RELATED(dim_person[Name]),
    ", ",bridge_content_person[order_no],ASC
)
```

#### Networks
> Top 3 networks by count, comma-separated.
```dax
VAR _bridgeRows = 
    RELATEDTABLE( bridge_content_network )
VAR _networkIDs =
    SELECTCOLUMNS( 
        _bridgeRows, 
        "@nid", bridge_content_network[network_id] 
    )
VAR _networksWithCount =
    ADDCOLUMNS(
        DISTINCT( _networkIDs ),
        "@name",  LOOKUPVALUE( dim_network[Networks], dim_network[network_id], [@nid] ),
        "@count", [Network#]
    )
VAR _top3 =
    TOPN( 3, _networksWithCount, [@count], DESC, [@name], ASC )
RETURN
    CONCATENATEX( _top3, [@name], ", ", [@count], DESC )
```

#### NetworksDisplay
> Null-safe wrapper: shows "-" if Networks is blank.
```dax
VAR currentNetworks = dim_content[Networks]
RETURN IF(ISBLANK(currentNetworks),"-",currentNetworks)
```

#### Production Companies
> Top 3 production companies by count (same pattern as Networks).
```dax
VAR _bridgeRows = 
    RELATEDTABLE( bridge_content_company )
VAR _companyIDs =
    SELECTCOLUMNS( 
        _bridgeRows, 
        "@nid", bridge_content_company[company_id] 
    )
VAR _companiesWithCount =
    ADDCOLUMNS(
        DISTINCT( _companyIDs ),
        "@name",  LOOKUPVALUE( dim_production_company[Prod. Companies], dim_production_company[company_id], [@nid] ),
        "@count", [Prod. Company#]
    )
VAR _top3 =
    TOPN( 3, _companiesWithCount, [@count], DESC, [@name], ASC )
RETURN
    CONCATENATEX( _top3, [@name], ", ", [@count], DESC )
```

#### RuntimeDisplay
> Formats movie runtime as "Xh Ym" string.
```dax
VAR TotalMinutes = dim_content[Runtime (Min.)]
VAR Hours = QUOTIENT(TotalMinutes,60)
VAR Minutes = MOD(TotalMinutes,60)
RETURN 
    SWITCH(
        TRUE(),
        ISBLANK(TotalMinutes),"-", 
        Hours=0,Minutes & "m",
        Minutes=0,Hours & "h",
        Hours & "h " & Minutes & "m"
    )
```
*Sorted by: `Runtime (Min.)`*

#### AVG_Episode_Runtime
> Average episode runtime for TV series (numeric).
```dax
VAR totalRuntime = 
CALCULATE(
    SUM(dim_episode[Runtime (Min.)]),
    NOT(ISBLANK(dim_episode[Runtime (Min.)]))
)
VAR totalEpisodes = 
CALCULATE(
    DISTINCTCOUNT(dim_episode[episode_key]),
    NOT(ISBLANK(dim_episode[Runtime (Min.)]))
)
RETURN ROUND(DIVIDE(totalRuntime,totalEpisodes),0)
```

#### AVG_Episode_Runtime_Display
> Formatted version ("Xh Ym") of average episode runtime.
```dax
VAR totalRuntime = CALCULATE(SUM(dim_episode[Runtime (Min.)]))
VAR totalEpisodes = CALCULATE(
    DISTINCTCOUNT(dim_episode[episode_key]),
    NOT(ISBLANK(dim_episode[Runtime (Min.)]))
)
VAR avgRuntime = ROUND(DIVIDE(totalRuntime,totalEpisodes),0)
VAR totalHours = QUOTIENT(avgRuntime,60)
VAR totalMinutes = MOD(avgRuntime,60)
RETURN 
    SWITCH(
        TRUE(),
        ISBLANK(avgRuntime) || totalEpisodes = 0 ,"-",
        totalHours=0,totalMinutes & "m",
        totalMinutes=0,totalHours & "h",
        totalHours & "h " & totalMinutes & "m"
    )
```
*Sorted by: `AVG_Episode_Runtime`*

#### RuntimeUnited
> Shows movie runtime or avg episode runtime depending on which is available.
```dax
VAR runtimeMovie = dim_content[RuntimeDisplay]
VAR runtimeSerie = dim_content[AVG_Episode_Runtime_Display]
RETURN 
IF(
    runtimeMovie = "-",
    runtimeSerie,
    runtimeMovie
)
```

#### TotalRuntime_Series
> Total episode runtime for a TV series.
```dax
CALCULATE(SUM(dim_episode[Runtime (Min.)]))
```

#### TotalRuntime_Series_Display
> Formatted version ("Xh Ym") of total series runtime.
```dax
VAR totalRuntime = dim_content[TotalRuntime_Series]
VAR totalHours = QUOTIENT(totalRuntime,60)
VAR totalMinutes = MOD(totalRuntime,60)
RETURN 
    SWITCH(
        TRUE(),
        ISBLANK(totalRuntime),"-",
        totalHours=0,totalMinutes & "m",
        totalMinutes=0,totalHours & "h",
        totalHours & "h " & totalMinutes & "m"
    )
```
*Sorted by: `TotalRuntime_Series`*

#### OverviewShort
> Truncates overview to 300 characters with ".." suffix.
```dax
IF(
    LEN(dim_content[Overview])>=300,
    LEFT(dim_content[Overview],300) & "..",
    dim_content[Overview]
)
```

#### TrailerDisplay
> Null-safe wrapper: shows "-" if Trailer is blank. Category: `WebUrl`.
```dax
VAR currentTrailer = dim_content[Trailer]
RETURN IF(ISBLANK(currentTrailer),"-",currentTrailer)
```

#### Oncoming Days-Left Display
> Calculates time remaining until release in "X yrs Y mths Z days" format.
```dax
VAR EndDate  = CALCULATE(FIRSTDATE(dim_content[Release Date]))
VAR StartDate = [Latest Popularity Date]

VAR TotalYears = DATEDIFF(StartDate, EndDate, YEAR) - 
                 IF(DATE(YEAR(EndDate), MONTH(StartDate), DAY(StartDate)) > EndDate, 1, 0) 
VAR TotalMonths = MOD(MONTH(EndDate) - MONTH(StartDate) + 12 - 
                  IF(DAY(EndDate) < DAY(StartDate), 1, 0), 12)
VAR TotalDays = INT(EndDate - EDATE(StartDate, TotalYears * 12 + TotalMonths))

VAR YearText = IF(TotalYears > 0, TotalYears & " yrs ", "")
VAR MonthText = IF(TotalMonths > 0, TotalMonths & " mths ", "")
VAR DayText = IF(TotalDays > 0, TotalDays & " days", "")

VAR CombinedText = TRIM(YearText & MonthText & DayText)
RETURN IF(CombinedText = "", "Same Day", CombinedText)
```

---

### dim_episode

#### RuntimeDisplay
> Formats episode runtime as "Xh Ym" string (same pattern as dim_content).
```dax
VAR TotalMinutes = dim_episode[Runtime (Min.)]
VAR Hours = QUOTIENT(TotalMinutes,60)
VAR Minutes = MOD(TotalMinutes,60)
RETURN 
    SWITCH(
        TRUE(),
        ISBLANK(TotalMinutes),"-", 
        Hours=0,Minutes & "m",
        Minutes=0,Hours & "h",
        Hours & "h " & Minutes & "m"
    )
```

#### TypeDisplay
> Maps TMDB episode types to friendly labels: `standard` → "Normal", `mid_season` → "Mid-Season", `finale` → "Season Final" or "Final" (if show has ended and it's the latest season).
```dax
VAR episodeType = [Episode Type]
VAR contentStatus = RELATED(dim_content[Status])
VAR currentContentID = [content_id]
VAR currentSeasonNo = dim_episode[Season No.]
VAR lastSeasonNo = 
    CALCULATE(
        MAX(dim_episode[Season No.]),
        ALLEXCEPT(dim_episode,dim_episode[content_id])
    )
RETURN
    SWITCH(
        TRUE(),
        ISBLANK(episodeType),"-",
        episodeType="standard","Normal",
        episodeType="mid_season","Mid-Season",
        episodeType="finale" && contentStatus="Ended" && lastSeasonNo=currentSeasonNo ,"Final",
        episodeType="finale","Season Final"
    )
```

---

### dim_season

#### AirDate DDMMYYYY
> Formats air date as "DD-MM-YYYY" string for display.
```dax
FORMAT(dim_season[Air Date].[Date],"DD-MM-YYYY","en")
```

#### OverviewShort
> Truncates season overview to 300 characters.
```dax
IF(
    LEN(dim_season[Overview])>=300,
    LEFT(dim_season[Overview],300) & "..",
    dim_season[Overview]
)
```

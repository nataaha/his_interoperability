CREATE OR REPLACE FUNCTION merge_organisationunits (source_uid character (11), dest_uid character (10), strategy character varying (50))
    RETURNS VOID
AS $$
DECLARE
    organisationunitid integer;
    has_children boolean;
    is_valid_strategy boolean;
    this_exists boolean;
BEGIN
    EXECUTE 'SELECT ''' || $1 || '''  IN (SELECT DISTINCT uid from organisationunit);' INTO this_exists;
    IF this_exists != TRUE THEN
        RAISE
        EXCEPTION 'Invalid source organisation unit';
    END IF;
    EXECUTE 'SELECT ''' || $2 || '''  IN (SELECT DISTINCT uid from organisationunit);' INTO this_exists;
    IF this_exists != TRUE THEN
        RAISE
        EXCEPTION 'Invalid destination organisation unit';
    END IF;
    EXECUTE 'SELECT ''' || $1 || ''' = ''' || $2 || '''' INTO this_exists;
    IF this_exists = TRUE THEN
        RAISE
        EXCEPTION 'Cannot self-merge';
    END IF;
    EXECUTE 'SELECT' || '''' || $3 || ''' NOT IN (''SUM'',''MAX'',''MIN'',''AVG'',''LAST'',''FIRST'')' INTO is_valid_strategy
    USING strategy;
    IF is_valid_strategy THEN
        RAISE
        EXCEPTION 'Please provide a merge strategy (SUM,MAX,MIN,AVG,LAST,FIRST)';
    END IF;
    EXECUTE 'SELECT organisationunitid from organisationunit where uid = ''' || $1 || '''' INTO organisationunitid;
    EXECUTE 'SELECT COUNT(organisationunitid) != 0 from organisationunit where parentid = $1' INTO has_children
    USING organisationunitid;
    IF has_children THEN
        RAISE
        EXCEPTION 'Organisationunit has children. Aborting.';
    END IF;
    --2.29 datavalue table schema
    CREATE TEMP TABLE _temp_merge_replacements (
        dataelementid integer,
        periodid integer,
        sourceid integer,
        categoryoptioncomboid integer,
        value character varying (50000),
        storedby character varying (255),
        lastupdated timestamp without time zone,
        comment character varying (50000),
        followup boolean,
        attributeoptioncomboid integer,
        created timestamp without time zone,
        deleted boolean)
    ON COMMIT DROP;
    -- Overlapping numeric values
    EXECUTE format('CREATE TEMP TABLE _temp_merge_overlaps
 	 ON COMMIT DROP AS
   SELECT a.*  FROM datavalue a
  INNER JOIN
  (SELECT dataelementid,periodid,
  categoryoptioncomboid,attributeoptioncomboid,COUNT(*) from datavalue
  WHERE sourceid in (
  select organisationunitid from organisationunit
  WHERE  uid IN ( %L,%L ) )
  AND deleted = FALSE
  GROUP BY dataelementid,periodid,categoryoptioncomboid,attributeoptioncomboid
  HAVING COUNT(*) > 1)  b on
  a.dataelementid = b.dataelementid
  AND
  a.periodid = b.periodid
  AND
  a.categoryoptioncomboid = b.categoryoptioncomboid
  AND
  a.attributeoptioncomboid = b.attributeoptioncomboid
  WHERE a.sourceid IN (
  select organisationunitid from organisationunit
  where uid IN ( %L,%L) )
  AND a.deleted  = FALSE
  and a.dataelementid in
  (SELECT DISTINCT dataelementid from
  dataelement where valuetype IN (''INTEGER'',''NUMBER'',''INTEGER_ZERO_OR_POSITIVE'',''INTEGER_POSITIVE''))', source_uid, dest_uid, source_uid, dest_uid);
    --Switch the source to the destination UID
    EXECUTE format('UPDATE _temp_merge_overlaps set sourceid =
  (select organisationunitid from organisationunit where uid =  %L )', dest_uid);
    --Insert into the staging table
    CASE strategy
    WHEN 'SUM',
        'MAX',
        'MIN',
        'AVG' THEN
        INSERT INTO _temp_merge_replacements (dataelementid, periodid, sourceid, categoryoptioncomboid, value, storedby, lastupdated, comment, followup, attributeoptioncomboid, created, deleted)
    SELECT
        dataelementid, periodid, sourceid, categoryoptioncomboid, CASE strategy
        WHEN 'SUM' THEN
            SUM(value::numeric)::character varying (50000)
            WHEN 'AVG' THEN
            AVG(value::numeric)::character varying (50000)
            WHEN 'MAX' THEN
            MAX(value::numeric)::character varying (50000)
            WHEN 'MIN' THEN
            MIN(value::numeric)::character varying (50000)
    END AS value, 'admin' AS storedby, now()::timestamp without time zone AS lastupdated, NULL::character varying (50000) AS comment, FALSE AS followup, attributeoptioncomboid, now()::timestamp without time zone AS created, FALSE AS deleted
FROM
    _temp_merge_overlaps
GROUP BY
    sourceid, dataelementid, periodid, categoryoptioncomboid, attributeoptioncomboid;
    -- Overlapping numeric values with LAST and FIRST operator
ELSE
    WITH ins AS (
        SELECT DISTINCT
            dataelementid,
            periodid,
            sourceid,
            categoryoptioncomboid,
            value,
            storedby,
            lastupdated,
            comment,
            followup,
            attributeoptioncomboid,
            created,
            deleted,
            CASE strategy
            WHEN 'FIRST' THEN
                rank()
                OVER (PARTITION BY
                        dataelementid,
                        periodid,
                        categoryoptioncomboid,
                        attributeoptioncomboid
                    ORDER BY
                        lastupdated ASC,
                        sourceid)
                    WHEN 'LAST' THEN
                    rank()
                    OVER (PARTITION BY
                            dataelementid,
                            periodid,
                            categoryoptioncomboid,
                            attributeoptioncomboid
                        ORDER BY
                            lastupdated DESC,
                            sourceid)
        END AS rnk
    FROM
        _temp_merge_overlaps)
INSERT INTO _temp_merge_replacements
SELECT
    dataelementid, periodid, sourceid, categoryoptioncomboid, value, storedby, lastupdated, comment, followup, attributeoptioncomboid, created, deleted
FROM
    ins
WHERE
    rnk = 1;
END CASE;
-- All overlapping data which is not numeric. The latest record will be taken.
-- In case of the exact time stamp (unlikely) prefer the destination
EXECUTE format('CREATE TEMP TABLE _temp_merge_overlaps_others
  ON COMMIT DROP AS
   SELECT a.*  FROM datavalue a
  INNER JOIN
  (SELECT dataelementid,periodid,categoryoptioncomboid,
  attributeoptioncomboid,COUNT(*) from datavalue
  WHERE sourceid in (
  select organisationunitid from organisationunit
  WHERE uid IN ( %L,%L ) )
  AND deleted = FALSE
  GROUP BY dataelementid,periodid,
  categoryoptioncomboid,attributeoptioncomboid
  HAVING COUNT(*) > 1 )  b on
  a.dataelementid = b.dataelementid AND
  a.periodid = b.periodid AND
  a.categoryoptioncomboid = b.categoryoptioncomboid AND
  a.attributeoptioncomboid = b.attributeoptioncomboid
  WHERE a.sourceid IN (
  SELECT organisationunitid from organisationunit
  WHERE uid IN ( %L,%L) )
  AND a.deleted = FALSE
  AND a.dataelementid IN (SELECT DISTINCT dataelementid
  from dataelement where valuetype
  NOT IN (''INTEGER'',''NUMBER'',''INTEGER_ZERO_OR_POSITIVE'',''INTEGER_POSITIVE''))', source_uid, dest_uid, source_uid, dest_uid);
EXECUTE format('
 WITH ins as (
  SELECT
                 dataelementid,
                 periodid,
                 sourceid,
                 categoryoptioncomboid,
                 value,
                 storedby,
                 lastupdated,
                 comment,
                 followup,
                 attributeoptioncomboid,
                 created,
                 deleted,
                 rank() OVER( PARTITION BY dataelementid,periodid,
                 categoryoptioncomboid,attributeoptioncomboid ORDER BY lastupdated DESC, sourceid) as rnk
         FROM
             _temp_merge_overlaps_others )
 INSERT INTO _temp_merge_replacements
 SELECT  dataelementid,
     periodid,
     (select organisationunitid from organisationunit where uid =  %L ) as sourceid,
     categoryoptioncomboid,
     value,
     storedby,
     now()::timestamp without time zone as lastupdated,
     comment,
     followup,
     attributeoptioncomboid,
     created,
     deleted from ins
     where rnk = 1', dest_uid);
--New records from the source which do not overlap at all
EXECUTE format('
 INSERT INTO _temp_merge_replacements
   SELECT
   a.dataelementid,
   a.periodid,
   (select organisationunitid from organisationunit where uid =  %L ) as sourceid,
   a.categoryoptioncomboid,
   a.value,
   a.storedby,
   now()::timestamp without time zone as lastupdated,
   a.comment,
   a.followup,
   a.attributeoptioncomboid,
   a.created,
   a.deleted
 FROM datavalue a
  INNER JOIN
  (SELECT dataelementid,periodid,categoryoptioncomboid,
  attributeoptioncomboid,COUNT(*) from datavalue
  WHERE sourceid in (
  select organisationunitid from organisationunit
  WHERE uid IN ( %L,%L ) )
  AND deleted = FALSE
  GROUP BY dataelementid,periodid,
  categoryoptioncomboid,attributeoptioncomboid
  HAVING COUNT(*) = 1 )  b on
  a.dataelementid = b.dataelementid
  AND
  a.periodid = b.periodid
  AND
  a.categoryoptioncomboid = b.categoryoptioncomboid
  AND
  a.attributeoptioncomboid = b.attributeoptioncomboid
  WHERE a.sourceid IN (
  SELECT organisationunitid from organisationunit
  WHERE uid IN ( %L,%L) )
  AND a.deleted = FALSE', dest_uid, source_uid, dest_uid, source_uid, dest_uid);
--Delete all data from the source and destination
EXECUTE format('DELETE FROM datavalue where sourceid in (
      SELECT organisationunitid from organisationunit WHERE uid IN ( %L,%L ))', dest_uid, source_uid);
--Insert the records back into the datavalue table
INSERT INTO datavalue
SELECT
    dataelementid, periodid, sourceid, categoryoptioncomboid, value, storedby, lastupdated, comment, followup, attributeoptioncomboid, created, deleted
FROM
    _temp_merge_replacements;
--Completely remove the source site
EXECUTE format('SELECT * FROM delete_site_with_data( %L )', source_uid);
END;
$$
LANGUAGE plpgsql VOLATILE;

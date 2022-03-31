SET SQLFORMAT csv 
SPOOL 'output/memory-info.csv';
SELECT
  a.memory_component,
  a.memory_size,
  a.parent_memory_component,
  a.parent_group,
  a.second_group,
  a.hasadvisory,
  a.isfixedsize,
  a.mc_type,
  a.factor_name,
  a.parameter,
  a.default_order,
  CASE
    WHEN a.hasadvisory = 'Y'
    AND a.memory_size = 0
    AND b.collection_status IS NULL THEN 'B'
    WHEN a.hasadvisory = 'Y'
    AND a.memory_size <> 0
    AND b.collection_status = 'N' THEN 'N'
    WHEN a.hasadvisory = 'Y' THEN 'Y'
    ELSE NULL
  END collection_status,
  CASE
    WHEN a.hasadvisory = 'Y' THEN CASE
      WHEN a.memory_component = 'Shared Pool' THEN a.memory_size
      WHEN a.memory_component = 'Java Pool' THEN a.memory_size
      WHEN a.memory_component LIKE '%Default%' THEN (
        SELECT
          DISTINCT granule_size /(1024 * 1024) granule_size
        FROM v$sga_dynamic_components
      )
      WHEN a.memory_component = 'PGA Aggregate Target' THEN 10
      ELSE 0
    END
  END minimum_size,
  CASE
    WHEN a.hasadvisory = 'Y' THEN CASE
      WHEN a.memory_component = 'PGA Aggregate Target' THEN (4096 * 1024) - 1
      ELSE (10 * 1024) - 1
    END
  END maximum_size,
  CASE
    WHEN (
      SELECT
        VALUE
      FROM v$parameter
      WHERE
        NAME = 'sga_target'
    ) > 0 THEN CASE
      WHEN a.memory_component = 'Shared Pool' THEN 'A'
      WHEN a.memory_component = 'Java Pool' THEN 'A'
      WHEN a.memory_component = 'Large Pool' THEN 'A'
      WHEN a.memory_component LIKE '%Default%' THEN 'A'
      ELSE 'M'
    END
    ELSE 'M'
  END sgamode
FROM (
    SELECT
      memory_component,
      memory_size,
      parent_memory_component,
      CASE
        WHEN parent_memory_component IS NULL THEN CASE
          WHEN memory_component = 'Fixed' THEN '1'
          WHEN memory_component = 'Variable' THEN '2'
          WHEN memory_component = 'Database Buffers' THEN '3'
          WHEN memory_component = 'Redo Buffers' THEN '4'
          WHEN memory_component = 'PGA Aggregate Target' THEN '5'
        END
        WHEN parent_memory_component = 'Variable' THEN '2'
        WHEN parent_memory_component = 'Database Buffers' THEN '3'
      END parent_group,
      CASE
        WHEN memory_component LIKE '% 2k%' THEN '1'
        WHEN memory_component LIKE '%4k%' THEN '2'
        WHEN memory_component LIKE '%8k%' THEN '3'
        WHEN memory_component LIKE '%16k%' THEN '4'
        WHEN memory_component LIKE '%32k%' THEN '5'
        WHEN memory_component LIKE '%Keep%' THEN '6'
        WHEN memory_component LIKE '%Recycle%' THEN '7'
        WHEN memory_component LIKE '%Shared%' THEN '1'
        WHEN memory_component LIKE '%Large%' THEN '2'
        WHEN memory_component LIKE '%Java%' THEN '3'
        WHEN memory_component LIKE '%Streams%' THEN '4'
        WHEN memory_component LIKE '%Other%' THEN '5'
        WHEN memory_component LIKE '%Free%' THEN '6'
        WHEN memory_component = 'Database Buffers' THEN '0'
        WHEN memory_component = 'Variable' THEN '0'
      END second_group,
      CASE
        WHEN memory_component LIKE '% 2k%' THEN 'Y'
        WHEN memory_component LIKE '%4k%' THEN 'Y'
        WHEN memory_component LIKE '%8k%' THEN 'Y'
        WHEN memory_component LIKE '%16k%' THEN 'Y'
        WHEN memory_component LIKE '%32k%' THEN 'Y'
        WHEN memory_component LIKE '%Keep%' THEN 'Y'
        WHEN memory_component LIKE '%Recycle%' THEN 'Y'
        WHEN memory_component LIKE '%Shared%' THEN 'Y'
        WHEN memory_component LIKE '%PGA%' THEN 'Y'
        WHEN memory_component LIKE '%Java%' THEN 'Y' --WHEN memory_component LIKE '%Streams%'
        --   THEN 'Y'
        ELSE 'N'
      END hasadvisory,
      CASE
        WHEN memory_component LIKE 'Fixed%' THEN 'Y'
        WHEN memory_component LIKE 'Variable:%' THEN 'Y'
        WHEN memory_component LIKE 'Redo%' THEN 'Y'
        ELSE 'N'
      END isfixedsize,
      CASE
        WHEN memory_component LIKE 'PGA%' THEN 'PGA'
        ELSE 'SGA'
      END mc_type,
      CASE
        WHEN memory_component LIKE '% 2k%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%4k%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%8k%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%16k%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%32k%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%Keep%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%Recycle%' THEN 'Relative Change in physical reads'
        WHEN memory_component LIKE '%Shared%' THEN 'Relative change in parse time loadings'
        WHEN memory_component LIKE '%Java%' THEN 'Relative change in parse time loadings'
        WHEN memory_component LIKE '%PGA%' THEN 'Cache hit percentage' --WHEN memory_component LIKE '%Streams%'
        --   THEN 'Dequeue rate'
        ELSE NULL
      END factor_name,
      CASE
        WHEN memory_component LIKE '% 2k%'
        AND memory_component LIKE '%Default%' THEN 'db_cache_size'
        WHEN memory_component LIKE '%4k%'
        AND memory_component LIKE '%Default%' THEN 'db_cache_size'
        WHEN memory_component LIKE '%8k%'
        AND memory_component LIKE '%Default%' THEN 'db_cache_size'
        WHEN memory_component LIKE '%16k%'
        AND memory_component LIKE '%Default%' THEN 'db_cache_size'
        WHEN memory_component LIKE '%32k%'
        AND memory_component LIKE '%Default%' THEN 'db_cache_size'
        WHEN memory_component LIKE '% 2k%'
        AND memory_component NOT LIKE '%Default%' THEN 'db_2k_cache_size'
        WHEN memory_component LIKE '%4k%'
        AND memory_component NOT LIKE '%Default%' THEN 'db_4k_cache_size'
        WHEN memory_component LIKE '%8k%'
        AND memory_component NOT LIKE '%Default%' THEN 'db_8k_cache_size'
        WHEN memory_component LIKE '%16k%'
        AND memory_component NOT LIKE '%Default%' THEN 'db_16k_cache_size'
        WHEN memory_component LIKE '%32k%'
        AND memory_component NOT LIKE '%Default%' THEN 'db_32k_cache_size'
        WHEN memory_component LIKE '%Keep%' THEN 'db_keep_cache_size'
        WHEN memory_component LIKE '%Recycle%' THEN 'db_recycle_cache_size'
        WHEN memory_component LIKE '%Shared%' THEN 'shared_pool_size'
        WHEN memory_component LIKE '%PGA%' THEN 'pga_aggregate_target'
        WHEN memory_component LIKE '%Java%' THEN 'java_pool_size'
        WHEN memory_component LIKE '%Large%' THEN 'large_pool_size'
        WHEN memory_component LIKE '%Streams%' THEN 'streams_pool_size'
        ELSE NULL
      END parameter,
      CASE
        WHEN memory_component LIKE '% 2k%'
        AND memory_component LIKE '%Default%' THEN 2
        WHEN memory_component LIKE '%4k%'
        AND memory_component LIKE '%Default%' THEN 2
        WHEN memory_component LIKE '%8k%'
        AND memory_component LIKE '%Default%' THEN 2
        WHEN memory_component LIKE '%16k%'
        AND memory_component LIKE '%Default%' THEN 2
        WHEN memory_component LIKE '%32k%'
        AND memory_component LIKE '%Default%' THEN 2
        WHEN memory_component LIKE '% 2k%'
        AND memory_component NOT LIKE '%Default%' THEN 6
        WHEN memory_component LIKE '%4k%'
        AND memory_component NOT LIKE '%Default%' THEN 7
        WHEN memory_component LIKE '%8k%'
        AND memory_component NOT LIKE '%Default%' THEN 8
        WHEN memory_component LIKE '%16k%'
        AND memory_component NOT LIKE '%Default%' THEN 9
        WHEN memory_component LIKE '%32k%'
        AND memory_component NOT LIKE '%Default%' THEN 10
        WHEN memory_component LIKE '%Keep%' THEN 4
        WHEN memory_component LIKE '%Recycle%' THEN 5
        WHEN memory_component LIKE '%Shared%' THEN 1
        WHEN memory_component LIKE '%PGA%' THEN 3
        WHEN memory_component LIKE '%Java%' THEN 11 --WHEN memory_component LIKE '%Streams%'
        --   THEN 12
        ELSE NULL
      END default_order
    FROM (
        SELECT
          REPLACE (NAME, ' Size') memory_component,
          ROUND (VALUE / 1048576, 2) memory_size,
          NULL parent_memory_component
        FROM v$sga
        UNION
        SELECT
          DECODE (
            component,
            'DEFAULT buffer cache',
            (
              SELECT
                'DB Cache (Default ' || VALUE / 1024 || 'k)'
              FROM v$parameter
              WHERE
                NAME = 'db_block_size'
            ),
            DECODE (
              INSTR (component, 'cache'),
              0,
              INITCAP (component),
              'DB ' || INITCAP (
                REPLACE (
                  REPLACE (
                    REPLACE (
                      component,
                      'DEFAULT '
                    ),
                    'blocksize buffer cache',
                    'cache'
                  ),
                  'buffer cache',
                  'cache'
                )
              )
            )
          ) memory_component,
          TO_NUMBER (current_size) / 1048576 memory_size,
          DECODE (
            INSTR (component, 'cache'),
            0,
            'Variable',
            'Database Buffers'
          ) parent_memory_component
        FROM v$sga_dynamic_components
        WHERE
          component <> 'OSM Buffer Cache'
          AND NVL (
            SUBSTR (
              component,
              INSTR (component, ' ') + 1,
              INSTR (component, 'K') - INSTR (component, ' ')
            ),
            'XX'
          ) <> (
            SELECT
              VALUE / 1024 || 'K'
            FROM v$parameter
            WHERE
              NAME = 'db_block_size'
          )
        UNION
        SELECT
          'PGA Aggregate Target',
          VALUE / (1024 * 1024),
          NULL
        FROM v$parameter
        WHERE
          NAME = 'pga_aggregate_target'
        UNION
        SELECT
          'Variable: Others' memory_component,
          ROUND (
            (c.VALUE - d.current_size - e.VALUE) / 1048576,
            2
          ) memory_size,
          'Variable' parent_memory_component
        FROM (
            SELECT
              NAME,
              VALUE
            FROM v$sga
            WHERE
              NAME = 'Variable Size'
          ) c,
          (
            SELECT
              current_size
            FROM v$sga_dynamic_free_memory
          ) d,
          (
            SELECT
              SUM (current_size) VALUE
            FROM v$sga_dynamic_components
            WHERE
              component IN (
                'shared pool',
                'large pool',
                'java pool',
                'streams pool'
              )
          ) e
        UNION
        SELECT
          'Free' memory_component,
          TO_NUMBER (current_size) / 1048576 memory_size,
          'Variable' parent_memory_component
        FROM v$sga_dynamic_free_memory
      )
  ) a,
  (
    SELECT
      a.memory_component,
      NVL (
        b.collection_status,
        a.collection_status
      ) collection_status
    FROM (
        SELECT
          CASE
            WHEN NAME = 'DEFAULT'
            AND b.bsize = block_size THEN 'DB Cache (Default ' || block_size / 1024 || 'k)'
            WHEN NAME <> 'DEFAULT' THEN 'DB ' || INITCAP (NAME) || ' Cache'
            ELSE 'DB ' || block_size / 1024 || 'k Cache'
          END memory_component,
          'Y' collection_status
        FROM v$db_cache_advice a,
          (
            SELECT
              VALUE bsize
            FROM v$parameter
            WHERE
              NAME = 'db_block_size'
          ) b
        WHERE
          size_factor = 1
      ) a,
      (
        SELECT
          CASE
            WHEN NAME = 'DEFAULT'
            AND b.bsize = block_size THEN 'DB Cache (Default ' || block_size / 1024 || 'k)'
            WHEN NAME <> 'DEFAULT' THEN 'DB ' || INITCAP (NAME) || ' Cache'
            ELSE 'DB ' || block_size / 1024 || 'k Cache'
          END memory_component,
          'N' collection_status
        FROM (
            SELECT
              NAME,
              block_size
            FROM v$db_cache_advice m
            WHERE
              estd_physical_read_factor = 1
              OR estd_physical_read_factor IS NULL
            GROUP BY
              NAME,
              block_size
            HAVING
              COUNT (*) = (
                SELECT
                  COUNT (*)
                FROM v$db_cache_advice
                WHERE
                  NAME = m.NAME
                  AND block_size = m.block_size
              )
          ) a,
          (
            SELECT
              VALUE bsize
            FROM v$parameter
            WHERE
              NAME = 'db_block_size'
          ) b
      ) b
    WHERE
      a.memory_component = b.memory_component(+)
    UNION
    SELECT
      a.memory_component,
      NVL (
        b.collection_status,
        a.collection_status
      ) collection_status
    FROM (
        SELECT
          'Shared Pool' memory_component,
          'Y' collection_status
        FROM v$shared_pool_advice
        WHERE
          shared_pool_size_factor = 1
      ) a,
      (
        SELECT
          'Shared Pool' memory_component,
          'N' collection_status
        FROM v$shared_pool_advice
        WHERE
          estd_lc_load_time_factor = 1
          OR estd_lc_load_time_factor IS NULL
        GROUP BY
          1
        HAVING
          COUNT (*) = (
            SELECT
              COUNT (*)
            FROM v$shared_pool_advice
          )
      ) b
    WHERE
      a.memory_component = b.memory_component(+)
    UNION
    SELECT
      a.memory_component,
      NVL (
        b.collection_status,
        a.collection_status
      ) collection_status
    FROM (
        SELECT
          'Java Pool' memory_component,
          'Y' collection_status
        FROM v$java_pool_advice
        WHERE
          java_pool_size_factor = 1
      ) a,
      (
        SELECT
          'Java Pool' memory_component,
          'N' collection_status
        FROM v$java_pool_advice
        WHERE
          estd_lc_load_time_factor = 1
          OR estd_lc_load_time_factor IS NULL
        GROUP BY
          1
        HAVING
          COUNT (*) = (
            SELECT
              COUNT (*)
            FROM v$java_pool_advice
          )
      ) b
    WHERE
      a.memory_component = b.memory_component(+)
    UNION
    SELECT
      a.memory_component,
      NVL (
        b.collection_status,
        a.collection_status
      ) collection_status
    FROM (
        SELECT
          'PGA Aggregate Target' memory_component,
          'Y' collection_status
        FROM v$pga_target_advice
        WHERE
          pga_target_factor = 1
      ) a,
      (
        SELECT
          'PGA Aggregate Target' memory_component,
          'N' collection_status
        FROM v$pga_target_advice
        WHERE
          (
            estd_pga_cache_hit_percentage = 100
            OR estd_pga_cache_hit_percentage IS NULL
            OR estd_pga_cache_hit_percentage = 0
          )
        GROUP BY
          1
        HAVING
          COUNT (*) = (
            SELECT
              COUNT (*)
            FROM v$pga_target_advice
          )
      ) b
    WHERE
      a.memory_component = b.memory_component(+)
  ) b
WHERE
  a.memory_component = b.memory_component(+)
ORDER BY
  a.parent_group,
  a.second_group;
SPOOL off;
SET  SQLFORMAT ansiconsole
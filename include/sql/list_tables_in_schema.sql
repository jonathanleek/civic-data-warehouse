select t.table_name
from information_schema.tables t
where t.table_schema = '{{params.schema}}'  -- put schema name here
      and t.table_type = 'BASE TABLE'
order by t.table_name;
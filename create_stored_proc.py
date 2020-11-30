#!/usr/bin/env python
# coding: utf-8
import sys
import pandas as pd
from Query import Query
from OrderedSet import OrderedSet

if __name__ == '__main__':
    
    spec = pd.read_excel(sys.argv[1], sheet_name=sys.argv[2]).fillna('')
    spec = spec[spec["Appl ID"] == sys.argv[3]]
    spec = spec[spec["Target Table Name.1"] == sys.argv[4]]
    spec = spec[spec["Stage Table"] == sys.argv[5]]
    spec = spec[spec["Derived/Verbati?"] != "Not Needed"]
    
    spec['Source DB Table'] = 'WDRDEV_ACTIVE_STAGE_DB.' + spec['Stage Table']
    spec['Target DB Table'] = 'WDRDEV_INTEGRATION_LAYER_DB.' + spec['Target Table Name.1']

    spec['SP NAME'] = 'fn_IL_' + spec['Appl ID'] + spec['Target Table Name.1'].str.upper() + '_' + spec['Stage Table'].str.lower()

    spec['Python Logic'] = spec.apply(lambda x : "Set as " + x["Target Table Name.1"] if x["Derived/Verbati?"] == "Verbatim" else x["Python Logic"], axis=1)

    Query.flush()
    for index, row in spec.iterrows():
        Query.parseColumn(row['Python Logic'], row['Target Attribute Name.1'], row['Source DB Table'])

    stored_proc = spec.groupby(['SP NAME'], sort=False)[['SP NAME']].agg(lambda x : '\n          ,'.join(OrderedSet(x.fillna(''))))
    stored_proc['Target DB Table'] = spec.groupby(['SP NAME'], sort=False)['Target DB Table'].agg(lambda x : '\n      '.join(OrderedSet(x.fillna(''))))
    stored_proc['Target Column'] = spec.groupby(['SP NAME'], sort=False)['Target Attribute Name.1'].agg(lambda x : '\n        ,'.join(OrderedSet(x.fillna(''))))

    stored_proc['Query'] = Query.genQuery().replace('\n','\n      ')
    stored_proc['Stored Procedure'] = 'CREATE OR REPLACE PROCEDURE ' + stored_proc['SP NAME'] + '\n  RETURNS VOID\n  LANGUAGE JAVASCRIPT\n  AS\n  $$\n    //Part 1 : Business Logic\n    var bus_query    = \'\n      INSERT INTO ' + stored_proc['Target DB Table'] + '\n      (\n        ' + stored_proc['Target Column'] + '\n      )\n      ' + stored_proc['Query'] + '\';\n    var statement   =    snowflake.createStatement( {sqlText: bus_query} );\n    statement.execute();\n  $$\n  ;'
    
    stored_proc.to_csv(stored_proc['SP NAME'].unique()[0] + ".sql", columns=['Stored Procedure'], header=None, index=None, sep=';', quotechar=' ')



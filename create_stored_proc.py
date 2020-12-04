#!/usr/bin/env python
# coding: utf-8
import sys
import pandas as pd
from Query import Query
from OrderedSet import OrderedSet

if __name__ == '__main__':
    
    spec = pd.read_excel(sys.argv[1], sheet_name=sys.argv[2], keep_default_na=False)
    spec = spec[spec["Appl ID"] == sys.argv[3]]
    spec = spec[spec["Target Table Name.1"] == sys.argv[4]]
    spec = spec[(spec["Data Set Name"] == sys.argv[5]) | (spec["Stage Table"] != "NA")]
    spec["Data Set Name"] = sys.argv[5]
    spec = spec[spec["Derived/Verbati?"] != "Not Needed"]
    spec = spec[spec["Source Column Description"] != "POSTLOAD"]
    
    spec['Source DB Table'] = 'WDRDEV_ACTIVE_STAGE_DB.' + spec['Appl ID'] + '.' + spec['Data Set Name']
    spec['Target Table'] = spec['Appl ID'] + '_IL_' + spec['Target Table Name.1'].str.upper() + '_' + spec['Data Set Name'].str.upper() + '_WORK'
    spec['Target DB Table'] = 'WDRDEV_INTEGRATION_LAYER_DB.WDR_IL_WORK.' + spec['Target Table']

    spec['SP NAME'] = 'SP_IL_' + spec['Appl ID'] + '_' + spec['Target Table Name.1'].str.upper() + '_' + spec['Data Set Name'].str.lower()

    spec['Python Logic'] = spec.apply(lambda x : "Set as " + x["Target Attribute Name.1"] if x["Derived/Verbati?"] == "Verbatim" else x["Python Logic"], axis=1)

    Query.flush()
    for index, row in spec.iterrows():
        Query.parseColumn(row['Python Logic'], row['Target Attribute Name.1'], row['Source DB Table'])

    stored_proc = spec.groupby(['SP NAME'], sort=False)[['SP NAME']].agg(lambda x : '\n          ,'.join(OrderedSet(x.fillna(''))))
    stored_proc['Target DB Table'] = spec.groupby(['SP NAME'], sort=False)['Target DB Table'].agg(lambda x : '\n      '.join(OrderedSet(x.fillna(''))))
    stored_proc['Target Column'] = spec.groupby(['SP NAME'], sort=False)['Target Attribute Name.1'].agg(lambda x : '\n        ,'.join(OrderedSet(x.fillna(''))))
    stored_proc['Target Table'] = spec.groupby(['SP NAME'], sort=False)['Target Table'].agg(lambda x : '\n        ,'.join(OrderedSet(x.fillna(''))))
    stored_proc['Target Table Name.1'] = spec.groupby(['SP NAME'], sort=False)['Target Table Name.1'].agg(lambda x : '\n        ,'.join(OrderedSet(x.fillna(''))))

    stored_proc['Query'] = Query.genQuery().replace('\n','\n      ')
    stored_proc['Stored Procedure'] = 'CREATE OR REPLACE PROCEDURE ' + stored_proc['SP NAME'] + '(BATCH_ID VARCHAR)\n  RETURNS VOID\n  LANGUAGE JAVASCRIPT\n  EXECUTE AS CALLER\n  AS\n  $$\n    //Part 1 : Business Logic\n    var bus_query    = \'\n      CREATE OR REPLACE TRANSIENT TABLE ' + stored_proc['Target DB Table'] + ' AS\n      ' + stored_proc['Query'] + '\';\n    var statement   =    snowflake.createStatement( {sqlText: bus_query} );\n    statement.execute();\n\n    var query2    = "CALL WDRDEV_COMMON_DB.CODE.REPO.SP_META_PERFORM_CDC(\'\'" + BATCH_ID + "\'\', \'\'' + stored_proc['Target Table'] + '\'\', \'\'' + stored_proc['Target Table Name.1'].str.upper() + '\'\', \'\'Type 2\'\')";\n    var statement   =    snowflake.createStatement( {sqlText: query2} );\n    statement.execute();\n  $$\n  ;'
    
    stored_proc.to_csv(stored_proc['SP NAME'].unique()[0] + ".sql", columns=['Stored Procedure'], header=None, index=None, sep=';', quotechar=' ')



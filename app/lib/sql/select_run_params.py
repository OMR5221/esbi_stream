
print("RUNNING PLANT_ID: " + str(plant_id))

es_dw_engine = create_engine('oracle://ES_DW_OWNER:+Ep8UcZB@PGSDWQ')
es_dw_conn = es_dw_engine.connect()

# Get tags and plant info:
try:
    es_tags_dim_res = es_dw_conn.execute("""
                                         select td.plant_id, td.tag_name, td.server_name,
                                         td.pi_method, pd.plant_code
                                         from ES_TAGS_DIM td
                                         JOIN ES_PLANT_DIM pd
                                            ON td.PLANT_ID = pd.PLANT_ID AND pd.status = 'A'
                                         WHERE td.PLANT_ID={PlantID}""".format(PlantID=plant_id))

except:
    print("Plant not found")
    raise

es_tags_df = pd.DataFrame(es_tags_dim_res.fetchall())
es_tags_df.columns = [k.upper() for k in es_tags_dim_res.keys()]

return es_tags_df
